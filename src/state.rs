use aws_sdk_sqs::Client;
use dashmap::DashMap;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::Deserialize;
use std::fmt;
use std::sync::Arc;

// 1. Core Data Structures
#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub arn: String,
    pub subscriptions: Vec<Subscription>,
    pub display_name: Option<String>,
    pub policy: Option<String>,
    pub delivery_policy: Option<String>,
    pub tracing_config: Option<String>,
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

// 3. SNS Actions
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnsRequest {
    pub action: String,
    pub name: Option<String>,
    #[serde(rename = "TopicArn")]
    pub topic_arn: Option<String>,
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

