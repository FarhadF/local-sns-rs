use axum::http::StatusCode;
use axum::response::Response;
use quick_xml::events::BytesText;
use quick_xml::Writer;
use std::io::Cursor;
use uuid::Uuid;

pub async fn error_response(code: &str, message: &str, status_code: StatusCode) -> Response {
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
