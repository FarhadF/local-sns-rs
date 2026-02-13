use crate::error::error_response;
use crate::responses::Member;
use crate::state::{Message, SharedState, SnsRequest, Subscription, Topic};
use aws_config::BehaviorVersion;
use axum::extract::{Form, State};
use axum::http::StatusCode;
use axum::response::Response;
use quick_xml::Writer;
use quick_xml::events::BytesText;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

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
        "GetTopicAttributes" => get_topic_attributes(State(state), params).await,
        "SetTopicAttributes" => set_topic_attributes(State(state), params).await,
        "ListTagsForResource" => list_tags_for_resource(State(state), params).await,
        "TagResource" => tag_resource(State(state), params).await,
        "UntagResource" => untag_resource(State(state), params).await,
        "GetSubscriptionAttributes" => get_subscription_attributes(State(state), params).await,
        "ListSubscriptionsByTopic" => list_subscriptions_by_topic(State(state), params).await,
        _ => {
            error_response(
                "InvalidAction",
                "Action not supported",
                StatusCode::BAD_REQUEST,
            )
            .await
        }
    }
}

pub async fn list_subscriptions_by_topic(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Topic ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let subscriptions = if let Some(topic) = state.topics.get(topic_name) {
        topic.subscriptions.clone()
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    };

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("ListSubscriptionsByTopicResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("ListSubscriptionsByTopicResult")
                .write_inner_content(|writer| {
                    writer
                        .create_element("Subscriptions")
                        .write_inner_content(|writer| {
                            for sub in subscriptions {
                                writer
                                    .create_element("member")
                                    .write_inner_content(|writer| {
                                        writer
                                            .create_element("TopicArn")
                                            .write_text_content(BytesText::new(&sub.arn))?;
                                        writer
                                            .create_element("Protocol")
                                            .write_text_content(BytesText::new(&sub.protocol))?;
                                        writer
                                            .create_element("SubscriptionArn")
                                            .write_text_content(BytesText::new(
                                                &sub.subscription_arn,
                                            ))?;
                                        writer
                                            .create_element("Owner")
                                            .write_text_content(BytesText::new("000000000000"))?;
                                        writer
                                            .create_element("Endpoint")
                                            .write_text_content(BytesText::new(&sub.endpoint))?;
                                        Ok(())
                                    })?;
                            }
                            Ok(())
                        })?;
                    // Pagination is not implemented, so NextToken is empty or omitted
                    // writer.create_element("NextToken").write_text_content(BytesText::new(""))?;
                    Ok(())
                })?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn get_subscription_attributes(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let subscription_arn = if let Some(subscription_arn) = params.subscription_arn {
        subscription_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Subscription ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let mut found_subscription = None;
    for topic in state.topics.iter() {
        if let Some(sub) = topic
            .subscriptions
            .iter()
            .find(|s| s.subscription_arn == subscription_arn)
        {
            found_subscription = Some(sub.clone());
            break;
        }
    }

    let subscription = if let Some(sub) = found_subscription {
        sub
    } else {
        return error_response("NotFound", "Subscription not found", StatusCode::NOT_FOUND).await;
    };

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("GetSubscriptionAttributesResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("GetSubscriptionAttributesResult")
                .write_inner_content(|writer| {
                    writer
                        .create_element("Attributes")
                        .write_inner_content(|writer| {
                            let attributes = vec![
                                ("SubscriptionArn", subscription.subscription_arn.as_str()),
                                ("TopicArn", subscription.arn.as_str()),
                                ("Owner", "000000000000"),
                                ("ConfirmationWasAuthenticated", "true"),
                                ("PendingConfirmation", "false"),
                                ("Protocol", subscription.protocol.as_str()),
                                ("Endpoint", subscription.endpoint.as_str()),
                            ];

                            for (key, value) in attributes {
                                writer
                                    .create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer
                                            .create_element("key")
                                            .write_text_content(BytesText::new(key))?;
                                        writer
                                            .create_element("value")
                                            .write_text_content(BytesText::new(value))?;
                                        Ok(())
                                    })?;
                            }
                            Ok(())
                        })?;
                    Ok(())
                })?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn list_tags_for_resource(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let resource_arn = if let Some(resource_arn) = params.resource_arn {
        resource_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Resource Arn",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = resource_arn.split(':').last().unwrap_or_default();

    let topic = if let Some(topic) = state.topics.get(topic_name) {
        topic
    } else {
        return error_response("NotFound", "Resource not found", StatusCode::NOT_FOUND).await;
    };

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("ListTagsForResourceResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("ListTagsForResourceResult")
                .write_inner_content(|writer| {
                    writer
                        .create_element("Tags")
                        .write_inner_content(|writer| {
                            for (key, value) in &topic.tags {
                                writer
                                    .create_element("member")
                                    .write_inner_content(|writer| {
                                        writer
                                            .create_element("Key")
                                            .write_text_content(BytesText::new(key))?;
                                        writer
                                            .create_element("Value")
                                            .write_text_content(BytesText::new(value))?;
                                        Ok(())
                                    })?;
                            }
                            Ok(())
                        })?;
                    Ok(())
                })?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn tag_resource(State(state): State<SharedState>, params: SnsRequest) -> Response {
    let resource_arn = if let Some(resource_arn) = params.resource_arn {
        resource_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Resource Arn",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let tags_entry = if let Some(tags_entry) = params.tags_entry {
        tags_entry
    } else {
        return error_response("InvalidParameter", "Missing Tags", StatusCode::BAD_REQUEST).await;
    };

    let topic_name = resource_arn.split(':').last().unwrap_or_default();

    if let Some(mut topic) = state.topics.get_mut(topic_name) {
        for tag in tags_entry {
            topic.tags.insert(tag.key, tag.value);
        }
    } else {
        return error_response("NotFound", "Resource not found", StatusCode::NOT_FOUND).await;
    };

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("TagResourceResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("TagResourceResult")
                .write_inner_content(|_| Ok(()))?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn untag_resource(State(state): State<SharedState>, params: SnsRequest) -> Response {
    let resource_arn = if let Some(resource_arn) = params.resource_arn {
        resource_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Resource Arn",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let tag_keys = if let Some(tag_keys) = params.tag_keys_entry {
        tag_keys
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Tag Keys",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = resource_arn.split(':').last().unwrap_or_default();

    if let Some(mut topic) = state.topics.get_mut(topic_name) {
        for key in tag_keys {
            topic.tags.remove(&key);
        }
    } else {
        return error_response("NotFound", "Resource not found", StatusCode::NOT_FOUND).await;
    };

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("UntagResourceResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("UntagResourceResult")
                .write_inner_content(|_| Ok(()))?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn create_topic(State(state): State<SharedState>, params: SnsRequest) -> Response {
    let name = if let Some(name) = params.name {
        name
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Topic Name",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let arn = format!("arn:aws:sns:us-east-1:000000000000:{}", name);

    let mut tags = HashMap::new();
    if let Some(tags_entry) = params.tags_entry {
        for tag in tags_entry {
            tags.insert(tag.key, tag.value);
        }
    }

    let topic = Topic {
        name: name.clone(),
        arn: arn.clone(),
        tags,
        subscriptions: vec![],
        display_name: None,
        policy: None,
        delivery_policy: None,
        tracing_config: None,
        firehose_failure_feedback_role_arn: None,
        firehose_success_feedback_role_arn: None,
        firehose_success_feedback_sample_rate: None,
        http_failure_feedback_role_arn: None,
        sqs_failure_feedback_role_arn: None,
        sqs_success_feedback_role_arn: None,
        sqs_success_feedback_sample_rate: None,
        http_success_feedback_role_arn: None,
        http_success_feedback_sample_rate: None,
        application_failure_feedback_role_arn: None,
        application_success_feedback_role_arn: None,
        application_success_feedback_sample_rate: None,
        lambda_failure_feedback_role_arn: None,
        lambda_success_feedback_role_arn: None,
        lambda_success_feedback_sample_rate: None,
        kms_master_key_id: None,
        signature_version: None,
        content_based_deduplication: None,
        fifo_topic: None,
        archive_policy: None,
        fifo_throughput_scope: None,
    };
    state.topics.insert(name, topic);

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("CreateTopicResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("CreateTopicResult")
                .write_inner_content(|writer| {
                    writer
                        .create_element("TopicArn")
                        .write_text_content(BytesText::new(&arn))?;
                    Ok(())
                })?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn delete_topic(State(state): State<SharedState>, params: SnsRequest) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Topic ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();
    state.topics.remove(topic_name);

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("DeleteTopicResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn list_topics(State(state): State<SharedState>) -> Response {
    let topics = state
        .topics
        .iter()
        .map(|topic_ref| Member {
            topic_arn: topic_ref.value().arn.clone(),
        })
        .collect::<Vec<_>>();

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("ListTopicsResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("ListTopicsResult")
                .write_inner_content(|writer| {
                    writer
                        .create_element("Topics")
                        .write_inner_content(|writer| {
                            for topic in topics {
                                writer
                                    .create_element("member")
                                    .write_inner_content(|writer| {
                                        writer
                                            .create_element("TopicArn")
                                            .write_text_content(BytesText::new(&topic.topic_arn))?;
                                        Ok(())
                                    })?;
                            }
                            Ok(())
                        })?;
                    writer
                        .create_element("NextToken")
                        .write_text_content(BytesText::new(""))?;
                    Ok(())
                })?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn set_topic_attributes(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Topic ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let attribute_name = if let Some(attribute_name) = params.attribute_name {
        attribute_name
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Attribute Name",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let attribute_value = if let Some(attribute_value) = params.attribute_value {
        attribute_value
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Attribute Value",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    if let Some(mut topic) = state.topics.get_mut(topic_name) {
        match attribute_name.as_str() {
            "DisplayName" => topic.display_name = Some(attribute_value),
            "Policy" => topic.policy = Some(attribute_value),
            "DeliveryPolicy" => topic.delivery_policy = Some(attribute_value),
            "TracingConfig" => topic.tracing_config = Some(attribute_value),
            "FirehoseSuccessFeedbackSampleRate" => {
                topic.firehose_success_feedback_sample_rate = Some(attribute_value)
            }
            "FirehoseFailureFeedbackRoleArn" => {
                topic.firehose_failure_feedback_role_arn = Some(attribute_value)
            }
            "FirehoseSuccessFeedbackRoleArn" => {
                topic.firehose_success_feedback_role_arn = Some(attribute_value)
            }
            "HTTPFailureFeedbackRoleArn" => {
                topic.http_failure_feedback_role_arn = Some(attribute_value)
            }
            "SQSSuccessFeedbackSampleRate" => {
                topic.sqs_success_feedback_sample_rate = Some(attribute_value)
            }
            "SQSFailureFeedbackRoleArn" => {
                topic.sqs_failure_feedback_role_arn = Some(attribute_value)
            }
            "SQSSuccessFeedbackRoleArn" => {
                topic.sqs_success_feedback_role_arn = Some(attribute_value)
            }
            "HTTPSuccessFeedbackSampleRate" => {
                topic.http_success_feedback_sample_rate = Some(attribute_value)
            }
            "HTTPSuccessFeedbackRoleArn" => {
                topic.http_success_feedback_role_arn = Some(attribute_value)
            }
            "ApplicationSuccessFeedbackSampleRate" => {
                topic.application_success_feedback_sample_rate = Some(attribute_value)
            }
            "ApplicationFailureFeedbackRoleArn" => {
                topic.application_failure_feedback_role_arn = Some(attribute_value)
            }
            "ApplicationSuccessFeedbackRoleArn" => {
                topic.application_success_feedback_role_arn = Some(attribute_value)
            }
            "LambdaSuccessFeedbackSampleRate" => {
                topic.lambda_success_feedback_sample_rate = Some(attribute_value)
            }
            "LambdaFailureFeedbackRoleArn" => {
                topic.lambda_failure_feedback_role_arn = Some(attribute_value)
            }
            "LambdaSuccessFeedbackRoleArn" => {
                topic.lambda_success_feedback_role_arn = Some(attribute_value)
            }
            "KmsMasterKeyId" => topic.kms_master_key_id = Some(attribute_value),
            "SignatureVersion" => topic.signature_version = Some(attribute_value),
            "ContentBasedDeduplication" => {
                topic.content_based_deduplication = Some(attribute_value)
            }
            "FifoTopic" => topic.fifo_topic = Some(attribute_value),
            "ArchivePolicy" => topic.archive_policy = Some(attribute_value),
            "FifoThroughputScope" => topic.fifo_throughput_scope = Some(attribute_value),
            _ => {
                return error_response(
                    "InvalidParameter",
                    "Attribute not supported",
                    StatusCode::BAD_REQUEST,
                )
                .await;
            }
        }
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    };

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("SetTopicAttributesResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn get_topic_attributes(
    State(state): State<SharedState>,
    params: SnsRequest,
) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Topic ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let topic = if let Some(topic) = state.topics.get(topic_name) {
        topic
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    };

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer.create_element("GetTopicAttributesResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer.create_element("GetTopicAttributesResult")
                .write_inner_content(|writer| {
                    writer.create_element("Attributes")
                        .write_inner_content(|writer| {
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("TopicArn"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(&topic.arn))?;
                                    Ok(())
                                })?;
                            if let Some(display_name) = &topic.display_name {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("DisplayName"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(display_name))?;
                                        Ok(())
                                    })?;
                            }
                            let policy = topic.policy.as_deref().unwrap_or_else(|| r#"{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[]}"#);
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("Policy"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(policy))?;
                                    Ok(())
                                })?;
                            if let Some(delivery_policy) = &topic.delivery_policy {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("DeliveryPolicy"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(delivery_policy))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(tracing_config) = &topic.tracing_config {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("TracingConfig"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(tracing_config))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(firehose_failure_feedback_role_arn) = &topic.firehose_failure_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("FirehoseFailureFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(firehose_failure_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(firehose_success_feedback_role_arn) = &topic.firehose_success_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("FirehoseSuccessFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(firehose_success_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            let firehose_success_feedback_sample_rate = topic.firehose_success_feedback_sample_rate.as_deref().unwrap_or("0");
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("FirehoseSuccessFeedbackSampleRate"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(firehose_success_feedback_sample_rate))?;
                                    Ok(())
                                })?;
                            if let Some(http_failure_feedback_role_arn) = &topic.http_failure_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("HTTPFailureFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(http_failure_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(sqs_failure_feedback_role_arn) = &topic.sqs_failure_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("SQSFailureFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(sqs_failure_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(sqs_success_feedback_role_arn) = &topic.sqs_success_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("SQSSuccessFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(sqs_success_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            let sqs_success_feedback_sample_rate = topic.sqs_success_feedback_sample_rate.as_deref().unwrap_or("0");
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("SQSSuccessFeedbackSampleRate"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(sqs_success_feedback_sample_rate))?;
                                    Ok(())
                                })?;
                            if let Some(http_success_feedback_role_arn) = &topic.http_success_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("HTTPSuccessFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(http_success_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            let http_success_feedback_sample_rate = topic.http_success_feedback_sample_rate.as_deref().unwrap_or("0");
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("HTTPSuccessFeedbackSampleRate"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(http_success_feedback_sample_rate))?;
                                    Ok(())
                                })?;
                            if let Some(application_failure_feedback_role_arn) = &topic.application_failure_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("ApplicationFailureFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(application_failure_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(application_success_feedback_role_arn) = &topic.application_success_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("ApplicationSuccessFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(application_success_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            let application_success_feedback_sample_rate = topic.application_success_feedback_sample_rate.as_deref().unwrap_or("0");
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("ApplicationSuccessFeedbackSampleRate"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(application_success_feedback_sample_rate))?;
                                    Ok(())
                                })?;
                            if let Some(lambda_failure_feedback_role_arn) = &topic.lambda_failure_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("LambdaFailureFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(lambda_failure_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(lambda_success_feedback_role_arn) = &topic.lambda_success_feedback_role_arn {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("LambdaSuccessFeedbackRoleArn"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(lambda_success_feedback_role_arn))?;
                                        Ok(())
                                    })?;
                            }
                            let lambda_success_feedback_sample_rate = topic.lambda_success_feedback_sample_rate.as_deref().unwrap_or("0");
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("LambdaSuccessFeedbackSampleRate"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(lambda_success_feedback_sample_rate))?;
                                    Ok(())
                                })?;
                            if let Some(kms_master_key_id) = &topic.kms_master_key_id {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("KmsMasterKeyId"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(kms_master_key_id))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(signature_version) = &topic.signature_version {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("SignatureVersion"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(signature_version))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(content_based_deduplication) = &topic.content_based_deduplication {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("ContentBasedDeduplication"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(content_based_deduplication))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(fifo_topic) = &topic.fifo_topic {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("FifoTopic"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(fifo_topic))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(archive_policy) = &topic.archive_policy {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("ArchivePolicy"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(archive_policy))?;
                                        Ok(())
                                    })?;
                            }
                            if let Some(fifo_throughput_scope) = &topic.fifo_throughput_scope {
                                writer.create_element("entry")
                                    .write_inner_content(|writer| {
                                        writer.create_element("key").write_text_content(BytesText::new("FifoThroughputScope"))?;
                                        writer.create_element("value").write_text_content(BytesText::new(fifo_throughput_scope))?;
                                        Ok(())
                                    })?;
                            }
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("SubscriptionsConfirmed"))?;
                                    writer.create_element("value").write_text_content(BytesText::new(topic.subscriptions.len().to_string().as_str()))?;
                                    Ok(())
                                })?;
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("SubscriptionsPending"))?;
                                    writer.create_element("value").write_text_content(BytesText::new("0"))?;
                                    Ok(())
                                })?;
                            writer.create_element("entry")
                                .write_inner_content(|writer| {
                                    writer.create_element("key").write_text_content(BytesText::new("SubscriptionsDeleted"))?;
                                    writer.create_element("value").write_text_content(BytesText::new("0"))?;
                                    Ok(())
                                })?;
                            Ok(())
                        })?;
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

pub async fn subscribe(State(state): State<SharedState>, params: SnsRequest) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Topic ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let endpoint = if let Some(endpoint) = params.endpoint {
        endpoint
    } else {
        return error_response(
            "InvalidParameter",
            "Missing endpoint",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let protocol = if let Some(protocol) = params.protocol {
        protocol
    } else {
        return error_response(
            "InvalidParameter",
            "Missing protocol",
            StatusCode::BAD_REQUEST,
        )
        .await;
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
    writer
        .create_element("SubscribeResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("SubscribeResult")
                .write_inner_content(|writer| {
                    writer
                        .create_element("SubscriptionArn")
                        .write_text_content(BytesText::new(&subscription_arn))?;
                    Ok(())
                })?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn unsubscribe(State(state): State<SharedState>, params: SnsRequest) -> Response {
    let subscription_arn = if let Some(subscription_arn) = params.subscription_arn {
        subscription_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Subscription ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_arn = subscription_arn.rsplitn(2, ':').nth(1).unwrap_or_default();
    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    if let Some(mut topic) = state.topics.get_mut(topic_name) {
        topic
            .subscriptions
            .retain(|s| s.subscription_arn != subscription_arn);
    } else {
        return error_response("NotFound", "Topic not found", StatusCode::NOT_FOUND).await;
    }

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("UnsubscribeResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}

pub async fn publish(State(state): State<SharedState>, params: SnsRequest) -> Response {
    let topic_arn = if let Some(topic_arn) = params.topic_arn {
        topic_arn
    } else {
        return error_response(
            "InvalidParameter",
            "Missing Topic ARN",
            StatusCode::BAD_REQUEST,
        )
        .await;
    };

    let topic_name = topic_arn.split(':').last().unwrap_or_default();

    let message_body = if let Some(message) = params.message {
        message
    } else {
        return error_response(
            "InvalidParameter",
            "Missing message",
            StatusCode::BAD_REQUEST,
        )
        .await;
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
                    format!(
                        "{}://{}:{}",
                        url.scheme(),
                        url.host_str().unwrap_or_default(),
                        url.port().unwrap_or(4566)
                    )
                } else {
                    "http://localhost:4566".to_string()
                };

                let sqs_client = if let Some(client) = state.sqs_clients.get(&endpoint_url) {
                    client.clone()
                } else {
                    let config = aws_config::defaults(BehaviorVersion::latest())
                        .endpoint_url(endpoint_url.clone())
                        .load()
                        .await;
                    let client = Arc::new(aws_sdk_sqs::Client::new(&config));
                    state
                        .sqs_clients
                        .insert(endpoint_url.clone(), client.clone());
                    client
                };

                match sqs_client
                    .send_message()
                    .queue_url(queue_url.clone())
                    .message_body(&message_body)
                    .send()
                    .await
                {
                    Ok(_) => tracing::info!("Message sent to SQS queue: {}", queue_url),
                    Err(e) => tracing::error!(
                        "Failed to send message to SQS queue: {}, error: {}",
                        queue_url,
                        e
                    ),
                }
            } else {
                tracing::info!(
                    "Sending message {:?} to endpoint {}",
                    message,
                    subscription.endpoint
                );
            }
        }
    } else {
        return error_response("NotFound", "Topic does not exist", StatusCode::NOT_FOUND).await;
    }

    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .create_element("PublishResponse")
        .with_attribute(("xmlns", "https://sns.amazonaws.com/doc/2010-03-31/"))
        .write_inner_content(|writer| {
            writer
                .create_element("PublishResult")
                .write_inner_content(|writer| {
                    writer
                        .create_element("MessageId")
                        .write_text_content(BytesText::new(&message_id))?;
                    Ok(())
                })?;
            writer
                .create_element("ResponseMetadata")
                .write_inner_content(|writer| {
                    writer
                        .create_element("RequestId")
                        .write_text_content(BytesText::new(&Uuid::new_v4().to_string()))?;
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap();

    let xml_response = writer.into_inner().into_inner();
    Response::builder()
        .header("Content-Type", "application/xml")
        .body(axum::body::Body::from(xml_response))
        .unwrap()
}
