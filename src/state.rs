use aws_sdk_sqs::Client;
use dashmap::DashMap;
use serde::Deserialize;
use std::sync::Arc;

// 1. Core Data Structures
#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub arn: String,
    pub subscriptions: Vec<Subscription>,
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

// 3. SNS Actions
#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct SnsRequest {
    pub action: String,
    pub name: Option<String>,
    pub topic_arn: Option<String>,
    pub endpoint: Option<String>,
    pub protocol: Option<String>,
    pub subscription_arn: Option<String>,
    pub message: Option<String>,
    pub subject: Option<String>,
}
