// CreateTopic
#[derive(Debug)]
pub struct CreateTopicResponse {
    pub create_topic_result: CreateTopicResult,
    pub response_metadata: ResponseMetadata,
}

#[derive(Debug)]
pub struct CreateTopicResult {
    pub topic_arn: String,
}

#[derive(Debug)]
pub struct ResponseMetadata {
    pub request_id: String,
}

// DeleteTopic
#[derive(Debug)]
pub struct DeleteTopicResponse {
    pub response_metadata: ResponseMetadata,
}

// ListTopics
#[derive(Debug)]
pub struct ListTopicsResponse {
    pub list_topics_result: ListTopicsResult,
    pub response_metadata: ResponseMetadata,
}

#[derive(Debug, Default)]
pub struct ListTopicsResult {
    pub topics: Topics,
    pub next_token: Option<String>,
}

#[derive(Debug, Default)]
pub struct Topics {
    pub member: Vec<Member>,
}

#[derive(Debug)]
pub struct Member {
    pub topic_arn: String,
}

#[derive(Debug)]
pub struct SubscribeResponse {
    pub subscribe_result: SubscribeResult,
    pub response_metadata: ResponseMetadata,
}

#[derive(Debug)]
pub struct SubscribeResult {
    pub subscription_arn: String,
}

#[derive(Debug)]
pub struct UnsubscribeResponse {
    pub response_metadata: ResponseMetadata,
}

#[derive(Debug)]
pub struct PublishResponse {
    pub publish_result: PublishResult,
    pub response_metadata: ResponseMetadata,
}

#[derive(Debug)]
pub struct PublishResult {
    pub message_id: String,
}

#[derive(Debug)]
pub struct GetTopicAttributesResponse {
    pub get_topic_attributes_result: GetTopicAttributesResult,
    pub response_metadata: ResponseMetadata,
}

#[derive(Debug, Default)]
pub struct GetTopicAttributesResult {
    pub attributes: Attributes,
}

#[derive(Debug, Default)]
pub struct Attributes {
    pub entry: Vec<Entry>,
}

#[derive(Debug)]
pub struct Entry {
    pub key: String,
    pub value: String,
}
