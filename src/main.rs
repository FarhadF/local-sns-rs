mod error;
mod handlers;
mod responses;
mod state;

use crate::handlers::handle_aws_request;
use crate::state::AppState;
use axum::Router;
use axum::routing::post;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    let shared_state = Arc::new(AppState {
        topics: DashMap::new(),
        sqs_clients: DashMap::new(),
    });

    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/", post(handle_aws_request))
        .with_state(shared_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 9911));
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
