use aws_sdk_sqs::Client;
use dashmap::DashMap;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

// 1. Core Data Structures
#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub arn: String,
    pub tags: HashMap<String, String>,
    pub subscriptions: Vec<Subscription>,
    pub display_name: Option<String>,
    pub policy: Option<String>,
    pub delivery_policy: Option<String>,
    pub tracing_config: Option<String>,
    pub firehose_failure_feedback_role_arn: Option<String>,
    pub firehose_success_feedback_role_arn: Option<String>,
    pub firehose_success_feedback_sample_rate: Option<String>,
    pub http_failure_feedback_role_arn: Option<String>,
    pub sqs_failure_feedback_role_arn: Option<String>,
    pub sqs_success_feedback_role_arn: Option<String>,
    pub sqs_success_feedback_sample_rate: Option<String>,
    pub http_success_feedback_role_arn: Option<String>,
    pub http_success_feedback_sample_rate: Option<String>,
    pub application_failure_feedback_role_arn: Option<String>,
    pub application_success_feedback_role_arn: Option<String>,
    pub application_success_feedback_sample_rate: Option<String>,
    pub lambda_failure_feedback_role_arn: Option<String>,
    pub lambda_success_feedback_role_arn: Option<String>,
    pub lambda_success_feedback_sample_rate: Option<String>,
    pub kms_master_key_id: Option<String>,
    pub signature_version: Option<String>,
    pub content_based_deduplication: Option<String>,
    pub fifo_topic: Option<String>,
    pub archive_policy: Option<String>,
    pub fifo_throughput_scope: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub endpoint: String,
    pub protocol: String,
    pub arn: String,
    pub subscription_arn: String,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    pub subject: Option<String>,
    pub body: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

// 2. In-Memory Storage
pub struct AppState {
    pub topics: DashMap<String, Topic>,
    pub sqs_clients: DashMap<String, Arc<Client>>,
}

pub type SharedState = Arc<AppState>;

#[derive(Debug, Deserialize, Default)]
pub struct AttributeEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct TagEntry {
    pub key: String,
    pub value: String,
}

// 3. SNS Actions
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnsRequest {
    pub action: String,
    pub name: Option<String>,
    #[serde(rename = "TopicArn")]
    pub topic_arn: Option<String>,
    #[serde(rename = "ResourceArn")]
    pub resource_arn: Option<String>,
    pub endpoint: Option<String>,
    pub protocol: Option<String>,
    #[serde(rename = "SubscriptionArn")]
    pub subscription_arn: Option<String>,
    pub message: Option<String>,
    pub subject: Option<String>,
    pub attribute_name: Option<String>,
    pub attribute_value: Option<String>,
    #[serde(flatten, deserialize_with = "deserialize_attributes")]
    pub attributes_entry: Option<Vec<AttributeEntry>>,
    #[serde(flatten, deserialize_with = "deserialize_tags")]
    pub tags_entry: Option<Vec<TagEntry>>,
    #[serde(flatten, deserialize_with = "deserialize_tag_keys")]
    pub tag_keys_entry: Option<Vec<String>>,
}

fn deserialize_attributes<'de, D>(deserializer: D) -> Result<Option<Vec<AttributeEntry>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct AttributesVisitor;

    impl<'de> Visitor<'de> for AttributesVisitor {
        type Value = Option<Vec<AttributeEntry>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map of attributes")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut attributes: Vec<AttributeEntry> = Vec::new();
            while let Some(key) = map.next_key::<String>()? {
                if key.starts_with("Attributes.entry.") {
                    let parts: Vec<&str> = key.split('.').collect();
                    if parts.len() == 4 {
                        let index = parts[2].parse::<usize>().unwrap_or(0);
                        if index > 0 {
                            while attributes.len() < index {
                                attributes.push(AttributeEntry::default());
                            }
                            let field = parts[3];
                            let value: String = map.next_value()?;
                            if field == "key" {
                                attributes[index - 1].key = value;
                            } else if field == "value" {
                                attributes[index - 1].value = value;
                            }
                        }
                    }
                } else {
                    let _: serde::de::IgnoredAny = map.next_value()?;
                }
            }
            if attributes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(attributes))
            }
        }
    }

    deserializer.deserialize_map(AttributesVisitor)
}

fn deserialize_tags<'de, D>(deserializer: D) -> Result<Option<Vec<TagEntry>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct TagsVisitor;

    impl<'de> Visitor<'de> for TagsVisitor {
        type Value = Option<Vec<TagEntry>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map of tags")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut tags: Vec<TagEntry> = Vec::new();
            while let Some(key) = map.next_key::<String>()? {
                if key.starts_with("Tags.member.") {
                    let parts: Vec<&str> = key.split('.').collect();
                    if parts.len() == 4 {
                        let index = parts[2].parse::<usize>().unwrap_or(0);
                        if index > 0 {
                            while tags.len() < index {
                                tags.push(TagEntry::default());
                            }
                            let field = parts[3];
                            let value: String = map.next_value()?;
                            if field == "Key" {
                                tags[index - 1].key = value;
                            } else if field == "Value" {
                                tags[index - 1].value = value;
                            }
                        }
                    }
                } else {
                    let _: serde::de::IgnoredAny = map.next_value()?;
                }
            }
            if tags.is_empty() {
                Ok(None)
            } else {
                Ok(Some(tags))
            }
        }
    }

    deserializer.deserialize_map(TagsVisitor)
}

fn deserialize_tag_keys<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct TagKeysVisitor;

    impl<'de> Visitor<'de> for TagKeysVisitor {
        type Value = Option<Vec<String>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a list of tag keys")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut keys: Vec<String> = Vec::new();
            while let Some(key) = map.next_key::<String>()? {
                if key.starts_with("TagKeys.member.") {
                    let parts: Vec<&str> = key.split('.').collect();
                    if parts.len() == 3 {
                        let index = parts[2].parse::<usize>().unwrap_or(0);
                        if index > 0 {
                            while keys.len() < index {
                                keys.push(String::new());
                            }
                            let value: String = map.next_value()?;
                            keys[index - 1] = value;
                        }
                    }
                } else {
                    let _: serde::de::IgnoredAny = map.next_value()?;
                }
            }
            if keys.is_empty() {
                Ok(None)
            } else {
                Ok(Some(keys))
            }
        }
    }

    deserializer.deserialize_map(TagKeysVisitor)
}
