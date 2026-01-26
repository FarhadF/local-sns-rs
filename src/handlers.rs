use crate::error::error_response;
use crate::state::{Message, SharedState, SnsRequest, Subscription, Topic};
use axum::extract::{Form, State};
use axum::http::StatusCode;
use axum::response::Response;
use quick_xml::events::BytesText;
use quick_xml::Writer;
use std::io::Cursor;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;
use crate::responses::Member;
use aws_config::BehaviorVersion;

pub async fn handle_aws_request(
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
        _ => error_response("InvalidAction", "Action not supported", StatusCode::BAD_REQUEST).await,
    }
}

pub async fn create_topic(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let name = if let Some(name) = params.name {
        name
    } else {
        return error_response("InvalidParameter", "Missing Topic Name", StatusCode::BAD_REQUEST).await;
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

pub async fn delete_topic(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response("InvalidParameter", "Missing Topic ARN", StatusCode::BAD_REQUEST).await;
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

pub async fn list_topics(
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

pub async fn subscribe(
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

    if let Some(mut topic) = state.topics.get_mut(topic_name) {
        topic.subscriptions.push(subscription);
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    };

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

pub async fn unsubscribe(
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

    if let Some(mut topic) = state.topics.get_mut(topic_name) {
        topic.subscriptions.retain(|s| s.subscription_arn != subscription_arn);
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    }

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

pub async fn publish(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response("InvalidParameter", "Missing Topic ARN", StatusCode::BAD_REQUEST).await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let message_body = if let Some(message) = params.message {
        message
    } else {
        return error_response("InvalidParameter", "Missing message", StatusCode::BAD_REQUEST).await;
    };

    let message_id = Uuid::new_v4().to_string();
    let message = Message {
        id: message_id.clone(),
        subject: params.subject,
        body: message_body.clone(),
        timestamp: chrono::Utc::now(),
    };

    if let Some(topic) = state.topics.get(topic_name) {
        for subscription in &topic.subscriptions {
            if subscription.protocol == "sqs" {
                let queue_url = subscription.endpoint.clone();
                let endpoint_url = if let Ok(url) = Url::parse(&queue_url) {
                    format!("{}://{}:{}", url.scheme(), url.host_str().unwrap_or_default(), url.port().unwrap_or(4566))
                } else {
                    "http://localhost:4566".to_string()
                };

                let sqs_client = if let Some(client) = state.sqs_clients.get(&endpoint_url) {
                    client.clone()
                } else {
                    let config = aws_config::defaults(BehaviorVersion::latest()).endpoint_url(endpoint_url.clone()).load().await;
                    let client = Arc::new(aws_sdk_sqs::Client::new(&config));
                    state.sqs_clients.insert(endpoint_url.clone(), client.clone());
                    client
                };

                match sqs_client
                    .send_message()
                    .queue_url(queue_url.clone())
                    .message_body(&message_body)
                    .send()
                    .await {
                    Ok(_) => tracing::info!("Message sent to SQS queue: {}", queue_url),
                    Err(e) => tracing::error!("Failed to send message to SQS queue: {}, error: {}", queue_url, e),
                }
            } else {
                tracing::info!("Sending message {:?} to endpoint {}", message, subscription.endpoint);
            }
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
