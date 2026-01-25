use axum::extract::Form;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Router;
use axum::routing::post;
use dashmap::DashMap;
use quick_xml::events::BytesText;
use serde::Deserialize;
use quick_xml::Writer;
use std::io::Cursor;
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
#[derive(Debug)]
struct CreateTopicResponse {
    create_topic_result: CreateTopicResult,
    response_metadata: ResponseMetadata,
}

#[derive(Debug)]
struct CreateTopicResult {
    topic_arn: String,
}

#[derive(Debug)]
struct ResponseMetadata {
    request_id: String,
}

// DeleteTopic
#[derive(Debug)]
struct DeleteTopicResponse {
    response_metadata: ResponseMetadata,
}

// ListTopics
#[derive(Debug)]
struct ListTopicsResponse {
    list_topics_result: ListTopicsResult,
    response_metadata: ResponseMetadata,
}

#[derive(Debug, Default)]
struct ListTopicsResult {
    topics: Topics,
    next_token: Option<String>,
}

#[derive(Debug, Default)]
struct Topics {
    member: Vec<Member>,
}

#[derive(Debug)]
struct Member {
    topic_arn: String,
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
        "ListTopics" => list_topics(State(state)).await,
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

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer.create_element("CreateTopicResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer.create_element("CreateTopicResult")
                .write_inner_content(|writer| {
                    writer.create_element("TopicArn").write_text_content(BytesText::new(&arn))?;
                    Ok(())
                })?;
            writer.create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer.create_element("RequestId").write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        }).unwrap();


    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
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

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer.create_element("DeleteTopicResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer.create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer.create_element("RequestId").write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        }).unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

async fn list_topics(
    State(state): State<SharedState>
) -> Response {
    let topics = state.topics
        .iter()
        .map(|topic_ref| Member {
            topic_arn: topic_ref.value().arn.clone(),
        })
        .collect::<Vec<_>>();

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer.create_element("ListTopicsResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer.create_element("ListTopicsResult")
                .write_inner_content(|writer| {
                    writer.create_element("Topics")
                        .write_inner_content(|writer| {
                            for topic in topics {
                                writer.create_element("member")
                                    .write_inner_content(|writer| {
                                        writer.create_element("TopicArn").write_text_content(BytesText::new(&topic.topic_arn))?;
                                        Ok(())
                                    })?;
                            }
                            Ok(())
                        })?;
                    writer.create_element("NextToken").write_text_content(BytesText::new(""))?;
                    Ok(())
                })?;
            writer.create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer.create_element("RequestId").write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        }).unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}
