//! Replay already-measured network completions, not a network simulator.
//! Input CSV: completion_us,destination,requested_qos,run_qos,size_mtu,rnl_us
use rpcstack_aequitas::{Class, Config, Controller, QosAssignment, RpcNetworkLatency};
use std::{
    io::{self, BufRead},
    time::Duration,
};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let controller = Controller::new(Config {
        classes: vec![
            Class {
                target_per_mtu: Duration::from_micros(15),
                percentile: 99.0,
            },
            Class {
                target_per_mtu: Duration::from_micros(25),
                percentile: 99.0,
            },
        ],
        alpha: 0.01,
        beta_per_mtu: 0.001,
        floor: 0.001,
    });
    let start = tokio::time::Instant::now();
    let mut previous = 0;
    println!("completion_us,destination,run_qos,p_admit");
    for line in io::stdin().lock().lines() {
        let line = line?;
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let f: Vec<_> = line.split(',').map(str::trim).collect();
        if f.len() != 6 {
            return Err("expected six CSV columns (no header)".into());
        }
        let elapsed: u64 = f[0].parse()?;
        if elapsed < previous {
            return Err("completion times must be ordered".into());
        }
        previous = elapsed;
        let assignment = QosAssignment {
            requested: f[2].parse()?,
            run: f[3].parse()?,
            size_mtu: f[4].parse()?,
        };
        controller.observe(
            f[1],
            assignment,
            RpcNetworkLatency(Duration::from_micros(f[5].parse()?)),
            start + Duration::from_micros(elapsed),
        )?;
        println!(
            "{},{},{},{:.6}",
            elapsed,
            f[1],
            assignment.run,
            controller.probability(f[1], assignment.run)
        );
    }
    Ok(())
}
