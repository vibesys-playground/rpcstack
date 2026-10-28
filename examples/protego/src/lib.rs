//! Protego (NSDI 2023), Algorithm 1 and ASQM, on rpcstack's credit transport.
use rpcstack_breakwater::{Config as CreditConfig, Controller as Credits};
pub use rpcstack_call::queue::{QueueBudget, QueueGate};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::{Response, Status};

#[derive(Clone, Debug)]
pub struct Config {
    pub efficiency: f64,
    pub maximum_drop: f64,
    pub increase: f64,
    pub decrease: f64,
    pub minimum: u64,
    pub maximum: u64,
    pub initial: u64,
    pub initial_rtt: Duration,
    pub queue_budget: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            efficiency: 0.1,
            maximum_drop: 1.0,
            increase: 1.0,
            decrease: 0.02,
            minimum: 1,
            maximum: 65536,
            initial: 8,
            initial_rtt: Duration::from_millis(10),
            queue_budget: Duration::from_millis(50),
        }
    }
}
#[derive(Clone, Copy, Default, Debug)]
pub struct Counts {
    pub incoming: u64,
    pub completed: u64,
    pub dropped: u64,
}
#[derive(Debug)]
pub struct Controller {
    config: Config,
    previous: Counts,
    credits: f64,
}
impl Controller {
    pub fn new(config: Config) -> Self {
        assert!(
            (0.0..=1.0).contains(&config.efficiency) && (0.0..=1.0).contains(&config.maximum_drop)
        );
        assert!(
            config.increase.is_finite()
                && config.increase > 0.0
                && config.decrease > 0.0
                && config.decrease < 1.0
        );
        assert!(
            config.minimum > 0
                && config.minimum <= config.initial
                && config.initial <= config.maximum
        );
        assert!(!config.initial_rtt.is_zero());
        Self {
            credits: config.initial as f64,
            config,
            previous: Counts::default(),
        }
    }
    pub fn update(&mut self, current: Counts) -> u64 {
        let di = current.incoming as f64 - self.previous.incoming as f64;
        let dout = current.completed as f64 - self.previous.completed as f64;
        if current.dropped as f64 <= self.config.maximum_drop * current.incoming as f64
            && di * dout > 0.0
            && dout.abs() > self.config.efficiency * di.abs()
        {
            self.credits += self.config.increase;
        } else {
            self.credits *= 1.0 - self.config.decrease;
        }
        self.credits = self
            .credits
            .clamp(self.config.minimum as f64, self.config.maximum as f64);
        self.previous = current;
        self.credits.floor() as u64
    }
}
#[derive(Debug)]
struct Window {
    counts: Counts,
    rtt: Duration,
}
/// Owns both controller and credit distribution. Start its loop once per server.
#[derive(Debug)]
pub struct Protego {
    pub credits: Arc<Credits>,
    controller: Mutex<Controller>,
    window: Mutex<Window>,
    queue_budget: Duration,
    started: std::sync::atomic::AtomicBool,
}
impl Protego {
    pub fn new(config: Config) -> Arc<Self> {
        let controller = Controller::new(config.clone());
        Arc::new(Self {
            credits: Credits::new(CreditConfig {
                initial: config.initial,
                maximum: config.maximum,
                // ASQM below owns shedding; disable Breakwater's first-poll AQM.
                target: Duration::from_secs(86400),
                aqm: Duration::from_secs(86400),
                ..CreditConfig::default()
            }),
            controller: Mutex::new(controller),
            window: Mutex::new(Window {
                counts: Counts::default(),
                rtt: config.initial_rtt,
            }),
            queue_budget: config.queue_budget,
            started: false.into(),
        })
    }
    pub fn start(self: &Arc<Self>) {
        if self
            .started
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(s) = weak.upgrade() else { break };
                let rtt = s.window.lock().unwrap().rtt;
                drop(s);
                // One settling RTT; collect a fresh three-RTT observation window.
                tokio::time::sleep(rtt).await;
                let Some(s) = weak.upgrade() else { break };
                s.window.lock().unwrap().counts = Counts::default();
                drop(s);
                tokio::time::sleep(rtt.saturating_mul(3)).await;
                let Some(s) = weak.upgrade() else { break };
                let counts = std::mem::take(&mut s.window.lock().unwrap().counts);
                let total = s.controller.lock().unwrap().update(counts);
                s.credits.set_total(total);
            }
        });
    }
    /// Wrap a handler and use its single budget at every instrumented lock.
    /// Service errors count as drops; cancelled handlers release their own RAII resources.
    pub async fn run<T, F, Fut>(&self, handler: F) -> Result<Response<T>, Status>
    where
        F: FnOnce(QueueBudget) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.window.lock().unwrap().counts.incoming += 1;
        let start = tokio::time::Instant::now();
        let result = handler(QueueBudget::new(self.queue_budget)).await;
        let mut window = self.window.lock().unwrap();
        if result.is_ok() {
            window.counts.completed += 1;
            window.rtt = (window.rtt.mul_f64(0.875) + start.elapsed().mul_f64(0.125))
                .max(Duration::from_millis(1));
        } else {
            window.counts.dropped += 1;
        }
        result
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paper_slope_drop_cap_sign_and_fractional_decrease() {
        let mut c = Controller::new(Config {
            initial: 100,
            increase: 1.0,
            maximum_drop: 0.5,
            ..Config::default()
        });
        assert_eq!(
            c.update(Counts {
                incoming: 100,
                completed: 100,
                dropped: 0
            }),
            101
        );
        assert_eq!(
            c.update(Counts {
                incoming: 200,
                completed: 105,
                dropped: 95
            }),
            98
        );
        assert_eq!(
            c.update(Counts {
                incoming: 100,
                completed: 150,
                dropped: 0
            }),
            97
        );
        assert_eq!(
            c.update(Counts {
                incoming: 200,
                completed: 250,
                dropped: 101
            }),
            95
        );
        let mut c = Controller::new(Config {
            initial: 8,
            ..Config::default()
        });
        for _ in 0..100 {
            c.update(Counts::default());
        }
        assert_eq!(c.update(Counts::default()), 1);
    }
}
