use crate::kafka::kafka_portal_worker::MAX_KAFKA_MESSAGE_SIZE;
use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

pub(crate) struct KafkaEncoder {
    read_half: DuplexStream,
    framed_write_half: FramedWrite<DuplexStream, LengthDelimitedCodec>,
}

impl KafkaEncoder {
    pub(crate) fn new() -> Self {
        //should be as big as the biggest kafka message we support, 4 bytes are the length 32bit field
        //we assume each cycle we write a kafka message and then immediately empty the buffer
        let (write_half, read_half) = tokio::io::duplex(MAX_KAFKA_MESSAGE_SIZE + 4);
        Self {
            read_half,
            framed_write_half: FramedWrite::new(
                write_half,
                LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_KAFKA_MESSAGE_SIZE)
                    .length_field_length(4)
                    .new_codec(),
            ),
        }
    }

    pub(crate) async fn write_kafka_message(
        &mut self,
        kafka_message: Vec<u8>,
    ) -> Result<(), std::io::Error> {
        self.framed_write_half
            .send(Bytes::from(kafka_message))
            .await
    }

    pub(crate) async fn read_length_encoded(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let mut buffer = BytesMut::new();
        let result = self.read_half.read(&mut buffer).await;

        if let Ok(read) = result {
            assert!(read > 0, "read must be called always after write");
            Ok(buffer.to_vec())
        } else {
            Err(result.err().unwrap())
        }
    }
}
