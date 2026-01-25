use axum::extract::Form;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Router;
use axum::routing::post;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber;
use uuid::Uuid;

// 1. Core Data Structures
#[derive(Debug, Clone)]
struct Topic {
    name: String,
    arn: String,
    subscriptions: Vec<Subscription>,
}

#[derive(Debug, Clone)]
struct Subscription {
    endpoint: String,
    protocol: String,
    arn: String,
}

#[derive(Debug, Clone)]
struct Message {
    id: String,
    subject: Option<String>,
    body: String,
    timestamp: chrono::DateTime<chrono::Utc>,
}

// 2. In-Memory Storage
struct AppState {
    topics: DashMap<String, Topic>,
}

type SharedState = Arc<AppState>;

// 3. SNS Actions
#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct SnsRequest {
    action: String,
    name: Option<String>,
    topic_arn: Option<String>,
}

// CreateTopic
#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct CreateTopicResponse {
    create_topic_result: CreateTopicResult,
    response_metadata: ResponseMetadata,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct CreateTopicResult {
    topic_arn: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct ResponseMetadata {
    request_id: String,
}

// DeleteTopic
#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct DeleteTopicResponse {
    response_metadata: ResponseMetadata,
}


#[tokio::main]
async fn main() {
    let shared_state = Arc::new(AppState {
        topics: DashMap::new(),
    });

    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/", post(handle_aws_request))
        .with_state(shared_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 9911));
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service()).await.unwrap();
}

async fn handle_aws_request(
    State(state): State<SharedState>,
    Form(params): Form<SnsRequest>,
) -> Response {
    match params.action.as_str() {
        "CreateTopic" => create_topic(State(state), params).await,
        "DeleteTopic" => delete_topic(State(state), params).await,
        _ => "Action not supported".into_response(),
    }
}

async fn create_topic(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let name = if let Some(name) = params.name {
        name
    } else {
        return "Missing Topic Name".into_response();
    };

    let arn = format!("arn:aws:sns:local:000000000000:{}", name);
    let topic = Topic {
        name: name.clone(),
        arn: arn.clone(),
        subscriptions: vec![],
    };
    state.topics.insert(name, topic);

    let response = CreateTopicResponse {
        create_topic_result: CreateTopicResult {
            topic_arn: arn,
        },
        response_metadata: ResponseMetadata {
            request_id: Uuid::new_v4().to_string(),
        },
    };

    let xml_response = serde_xml_rs::to_string(&response).unwrap();
    xml_response.into_response()
}

async fn delete_topic(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return "Missing Topic ARN".into_response();
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();
    state.topics.remove(topic_name);

    let response = DeleteTopicResponse {
        response_metadata: ResponseMetadata {
            request_id: Uuid::new_v4().to_string(),
        },
    };

    let xml_response = serde_xml_rs::to_string(&response).unwrap();
    xml_response.into_response()
}
