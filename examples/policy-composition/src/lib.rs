//! Executable evidence that independent RPC modules and async call policies compose.
#[cfg(test)]
mod tests {
    use rpcstack_breakwater::{
        BreakwaterModule, BreakwaterServer, Config, Controller, CreditClient, CreditService,
    };
    use rpcstack_c3::{C3Module, SelectedCall, Selector};
    use rpcstack_call::{execute, CallInfo, Stage};
    use rpcstack_dagor::{DagorCall, DagorModule, PeerThresholds, Priority};
    use std::{sync::atomic::Ordering, time::Duration};
    type Stack = rpcstack::policy_stack![DagorModule, BreakwaterModule, C3Module];
    #[tokio::test]
    async fn three_modules_two_async_policies_preserve_wire_and_release_on_rejection() {
        let controller = Controller::new(Config {
            initial: 1,
            maximum: 1,
            target: Duration::from_secs(1),
            aqm: Duration::from_secs(2),
            ..Config::default()
        });
        let context =
            rpcstack_tonic::ServerContext::<Stack>::try_with_resources("probe.Probe", |init| {
                init.provide(BreakwaterServer(controller.clone()))
            })
            .unwrap();
        let app = rpcstack_testkit::serve_with_stage(context, None, Some(Stage::new(1))).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_address = format!("http://{}", listener.local_addr().unwrap());
        let control = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(rpcstack_breakwater::pb::credits_server::CreditsServer::new(
                    CreditService(controller),
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let credits = CreditClient::connect(control_address, 1).await.unwrap();
        let selector = Selector::new(rpcstack_c3::Config::default(), [app.address.clone()]);
        let mut client = rpcstack_testkit::connect::<Stack>(&app.address).await;
        let peers = PeerThresholds::default();
        for accepted in [true, false, true] {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let selection = tokio::time::timeout_at(
                deadline,
                selector.select(std::slice::from_ref(&app.address)),
            )
            .await
            .unwrap()
            .unwrap();
            let selected = SelectedCall::new(selection);
            let info = CallInfo {
                destination: app.address.clone(),
                ordinal: 0,
                deadline: Some(deadline),
            };
            let mut request = tonic::Request::new(rpcstack_testkit::input());
            if accepted {
                request.extensions_mut().insert(DagorCall {
                    priority: Priority::new(1, 1).unwrap(),
                    peers: peers.clone(),
                });
            }
            // C3 timing starts after credit admission; both tickets own cleanup.
            let result = execute(&credits, &info, request, |request| {
                execute(&selected, &info, request, |request| client.call(request))
            })
            .await;
            if accepted {
                let response = result.unwrap();
                let mut inbound = tonic::metadata::MetadataMap::new();
                inbound.insert("ctx", response.get_ref().wire.parse().unwrap());
                let wire = rpcstack::WireIn::from_metadata(&inbound).unwrap();
                assert!(wire.get::<DagorModule>().unwrap().is_some());
                assert!(wire.get::<BreakwaterModule>().unwrap().is_some());
                let feedback = rpcstack::WireIn::from_metadata(response.metadata()).unwrap();
                assert!(feedback.get::<C3Module>().unwrap().is_some());
                assert!(feedback.get::<BreakwaterModule>().unwrap().is_some());
                assert!(feedback.get::<DagorModule>().unwrap().is_some());
            } else {
                assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
            }
            assert_eq!(selector.pending(&app.address), 0);
            assert_eq!(credits.inflight(), 0);
        }
        assert_eq!(app.calls.load(Ordering::Relaxed), 2);
        assert!(selector.score(&app.address) > 0.0);
        control.abort();
    }
}
#[cfg(test)]
mod masa_transport {
    use masa_policy::{ContextBuilder, MasaRequestExt, MasaStack};
    #[tokio::test]
    async fn optional_masa_stack_runs_on_generic_two_hop_transport() {
        let leaf = rpcstack_testkit::serve(
            rpcstack_tonic::ServerContext::<MasaStack>::try_new("leaf").unwrap(),
            None,
        )
        .await;
        let downstream = tonic::transport::Endpoint::from_shared(leaf.address.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let front = rpcstack_testkit::serve(
            rpcstack_tonic::ServerContext::<MasaStack>::try_new("front").unwrap(),
            Some(downstream),
        )
        .await;
        let mut client = rpcstack_testkit::connect::<MasaStack>(&front.address).await;
        let now = masa_core::time_now();
        let context = ContextBuilder::new("probe", 7)
            .gateway_entry(now)
            .slo(10_000_000)
            .deadline(now + 10_000_000)
            .build();
        let request = tonic::Request::new(rpcstack_testkit::input()).with_masa_context(&context);
        let response = client.call(request).await.unwrap();
        let mut wire = tonic::metadata::MetadataMap::new();
        wire.insert("ctx", response.get_ref().wire.parse().unwrap());
        let leaf_context = masa_policy::get_masa_context_from_metadata(&wire).unwrap();
        assert_eq!(leaf_context, context);
        assert_eq!(
            masa_policy::get_masa_context_from_metadata(response.metadata()).unwrap(),
            context
        );
        assert_eq!(front.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(leaf.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
