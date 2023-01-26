use bytes::{Buf, BytesMut};
use kafka_protocol::messages::{ApiKey, MetadataRequest, RequestHeader};
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::Decodable;
use tracing::info;

pub(crate) fn peek_requests(mut original: BytesMut) -> BytesMut {
    if original.len() < 4 {
        warn!("cannot decode kafka header: buffer is too small");
        return original;
    }

    //let's clone the view of the buffer without cloning the content
    let mut buffer = original.peek_bytes(0..original.len());

    let version = buffer.peek_bytes(2..4).get_i16();
    let result = RequestHeader::decode(&mut buffer, version);
    if let Err(_err) = result {
        //the error doesn't contain any useful information
        //just returning the original buffer
        warn!("cannot decode kafka header");
        return original;
    }
    let header = result.unwrap();

    if let Ok(api_key) = ApiKey::try_from(header.request_api_key) {
        info!(
            "request: length: {}, version {:?}, api {:?}",
            buffer.len(),
            header.request_api_version,
            api_key
        );

        match api_key {
            ApiKey::MetadataKey => {
                let result = MetadataRequest::decode(&mut buffer, version);
                if let Err(_err) = result {
                    //the error doesn't contain any useful information
                    //just returning the original buffer
                    warn!("cannot decode kafka metadata request");
                    return original;
                }
                let request = result.unwrap();
                info!(
                    "request: topics={:?}; auto_creation={:?}",
                    request.topics, request.allow_auto_topic_creation
                );
            }
            ApiKey::UpdateMetadataKey => {}
            _ => {}
        }
    } else {
        warn!("unknown request api: {:?}", header.request_api_key);
    }

    original
}
