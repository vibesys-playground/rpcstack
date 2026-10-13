use rpcstack::{Extensions, MissingDependency, Module, ModuleServer, ServerInit, WireIn};
use rpcstack_tonic::{ParentContext, ServerContext};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tonic::rpcstack::ParentHooks;
use tonic::{CowGrpcMethod, GrpcMethod};

#[derive(Debug, Clone)]
struct Counter(Arc<AtomicUsize>);
#[derive(Debug)]
struct Server(Counter);
impl ModuleServer for Server {
    fn new(init: &mut ServerInit) -> Result<Self, MissingDependency> {
        Ok(Self(init.require::<Counter>()?))
    }
}
#[derive(Debug)]
struct ModuleWithResource;
impl Module for ModuleWithResource {
    type Server = Server;
    type Wire = ();
    const NAME: &'static str = "resource";
    fn new(_: &CowGrpcMethod, server: &Server, _: &WireIn<'_>, _: &mut Extensions) -> Self {
        server.0 .0.fetch_add(1, Ordering::Relaxed);
        Self
    }
}
type Stack = rpcstack::policy_stack![ModuleWithResource];
#[test]
fn resources_are_required_and_isolated_per_constructed_server() {
    assert!(ServerContext::<Stack>::try_new("test").is_err());
    let first = Counter(Arc::new(AtomicUsize::new(0)));
    let second = Counter(Arc::new(AtomicUsize::new(0)));
    let a = ServerContext::<Stack>::try_with_resources("test", |init| init.provide(first.clone()))
        .unwrap();
    let b = ServerContext::<Stack>::try_with_resources("test", |init| init.provide(second.clone()))
        .unwrap();
    let request = http::Request::new(());
    let _ = ParentContext::<Stack>::begin(GrpcMethod::new("test", "call"), &request, Arc::new(a));
    assert_eq!(first.0.load(Ordering::Relaxed), 1);
    assert_eq!(second.0.load(Ordering::Relaxed), 0);
    let _ = ParentContext::<Stack>::begin(GrpcMethod::new("test", "call"), &request, Arc::new(b));
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
}
