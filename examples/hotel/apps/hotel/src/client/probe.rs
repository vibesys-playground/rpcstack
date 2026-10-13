//! Semantic checks for the benchmark model (whose user gate is probabilistic).
mod frontend {
    tonic::include_proto!("frontend");
}
use frontend::{frontend_client::FrontendClient, PingRequest, ReservationRequest, SearchRequest};
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Client,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args().nth(1).expect("frontend address");
    let mongo = std::env::args().nth(2).expect("reservation Mongo address");
    let mut client = FrontendClient::connect(address).await?;
    let token = format!("rpcstack-probe-{}", std::process::id());
    assert_eq!(
        client
            .handle_ping(PingRequest {
                message: token.clone()
            })
            .await?
            .into_inner()
            .message,
        token
    );
    let response = client
        .handle_search(SearchRequest {
            lat: 37.7867,
            lon: -122.4112,
            in_date: "2015-04-09".into(),
            out_date: "2015-04-10".into(),
            locale: None,
        })
        .await?
        .into_inner();
    assert!(
        !response.hotels.is_empty(),
        "search must execute the real downstream chain"
    );
    assert!(response
        .hotels
        .iter()
        .all(|h| !h.id.is_empty() && !h.name.is_empty()));
    let request = ReservationRequest {
        username: "Username_1".into(),
        password: "Password_1".into(),
        customer: token.clone(),
        hotels: vec!["1".into()],
        in_date: "2015-04-29".into(),
        out_date: "2015-04-30".into(),
        num_rooms: 1,
    };
    // The inherited workload model admits 80% of user checks, independently of
    // credentials. Empty responses are legitimate. Require real reservations
    // and verify their persisted count instead of assuming authentication.
    let mut accepted = 0;
    for _ in 0..400 {
        let response = client
            .handle_reservation(request.clone())
            .await?
            .into_inner();
        if !response.hotels.is_empty() {
            assert_eq!(response.hotels, ["1"]);
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, 200,
        "must enforce hotel 1's capacity of 200 rooms"
    );
    let db = Client::with_uri_str(mongo).await?;
    let docs: Vec<Document> = db
        .database("reservation-db")
        .collection("reservation")
        .find(doc! {"customerName": &token}, None)
        .await?
        .try_collect()
        .await?;
    let rooms: i32 = docs.iter().map(|d| d.get_i32("number").unwrap()).sum();
    assert_eq!(rooms, 200, "successful RPCs must persist the reservations");
    println!("semantic checks passed: ping, search, persisted reservation, capacity");
    Ok(())
}
