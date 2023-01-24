use bytes::{Buf, BytesMut};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, RequestHeader, RequestKind};
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio_util::codec;
use tracing::info;

pub(crate) fn transform_if_necessary(mut buffer: BytesMut) -> BytesMut {
    let version = buffer.peek_bytes(2..4).get_i16();
    let header = RequestHeader::decode(&mut buffer, version).unwrap();
    let api_key = ApiKey::try_from(header.request_api_version).unwrap();

    info!(
        "request: length: {}, version {:?}, api {:?}",
        buffer.len(),
        version,
        api_key
    );

    buffer
}
