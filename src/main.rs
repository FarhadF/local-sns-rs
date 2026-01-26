use axum::extract::Form;
use axum::extract::State;
use axum::http::StatusCode;
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
    subscription_arn: String,
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
    endpoint: Option<String>,
    protocol: Option<String>,
    subscription_arn: Option<String>,
    message: Option<String>,
    subject: Option<String>,
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

#[derive(Debug)]
struct SubscribeResponse {
    subscribe_result: SubscribeResult,
    response_metadata: ResponseMetadata,
}

#[derive(Debug)]
struct SubscribeResult {
    subscription_arn: String,
}

#[derive(Debug)]
struct UnsubscribeResponse {
    response_metadata: ResponseMetadata,
}

#[derive(Debug)]
struct PublishResponse {
    publish_result: PublishResult,
    response_metadata: ResponseMetadata,
}

#[derive(Debug)]
struct PublishResult {
    message_id: String,
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
        "Subscribe" => subscribe(State(state), params).await,
        "Unsubscribe" => unsubscribe(State(state), params).await,
        "Publish" => publish(State(state), params).await,
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

async fn subscribe(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response("InvalidParameter", "Missing Topic ARN", StatusCode::BAD_REQUEST).await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let endpoint = if let Some(endpoint) = params.endpoint {
        endpoint
    } else {
        return error_response("InvalidParameter", "Missing endpoint", StatusCode::BAD_REQUEST).await;
    };

    let protocol = if let Some(protocol) = params.protocol {
        protocol
    } else {
        return error_response("InvalidParameter", "Missing protocol", StatusCode::BAD_REQUEST).await;
    };

    let subscription_arn = format!("{}:{}", topic_arn, Uuid::new_v4());

    let subscription = Subscription {
        endpoint,
        protocol,
        arn: topic_arn.clone(),
        subscription_arn: subscription_arn.clone(),
    };

    let mut topic = if let Some(mut topic) = state.topics.get_mut(topic_name) {
        topic
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    };

    topic.subscriptions.push(subscription);

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer.create_element("SubscribeResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer.create_element("SubscribeResult")
                .write_inner_content(|writer| {
                    writer.create_element("SubscriptionArn").write_text_content(BytesText::new(&subscription_arn))?;
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

async fn unsubscribe(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let subscription_arn = if let Some(subscription_arn) = params.subscription_arn {
        subscription_arn
    } else {
        return error_response("InvalidParameter", "Missing Subscription ARN", StatusCode::BAD_REQUEST).await;
    };

    let topic_arn = subscription_arn.rsplitn(2, ':').nth(1).unwrap_or_default();
    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let mut topic = if let Some(mut topic) = state.topics.get_mut(topic_name) {
        topic
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    };

    topic.subscriptions.retain(|s| s.subscription_arn != subscription_arn);

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer.create_element("UnsubscribeResponse")
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

async fn error_response(code: &str, message: &str, status_code: StatusCode) -> Response {
    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("ErrorResponse")
        .with_attribute(("xmlns", "http://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer.create_element("Error").write_inner_content(|writer| {
                writer.create_element("Type").write_text_content(BytesText::new("Sender"))?;
                writer.create_element("Code").write_text_content(BytesText::new(code))?;
                writer.create_element("Message").write_text_content(BytesText::new(message))?;
                Ok(())
            })?;
            writer.create_element("RequestId").write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .status(status_code)
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

async fn publish(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return "Missing Topic ARN".into_response();
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let message_body = if let Some(message) = params.message {
        message
    } else {
        return "Missing message".into_response();
    };

    let message_id = Uuid::new_v4().to_string();
    let message = Message {
        id: message_id.clone(),
        subject: params.subject,
        body: message_body,
        timestamp: chrono::Utc::now(),
    };

    if let Some(topic) = state.topics.get(topic_name) {
        for subscription in &topic.subscriptions {
            tracing::info!("Sending message {:?} to endpoint {}", message, subscription.endpoint);
        }
    } else {
        return error_response("NotFound", "Topic does not exist", StatusCode::NOT_FOUND).await;
    }

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer.create_element("PublishResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer.create_element("PublishResult")
                .write_inner_content(|writer| {
                    writer.create_element("MessageId").write_text_content(BytesText::new(&message_id))?;
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
