use crate::kafka::kafka_portal_worker::MAX_KAFKA_MESSAGE_SIZE;
use bytes::BytesMut;
use futures::Stream;
use ockam::compat::tokio;
use ockam_transport_tcp::MAX_PAYLOAD_SIZE;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::task::Poll;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

/// internal util, pass through to decode length delimited kafka packages
/// keeps its own internal buffer
pub(crate) struct KafkaDecoder {
    write_half: DuplexStream,
    framed_read_half: FramedRead<DuplexStream, LengthDelimitedCodec>,
}

impl KafkaDecoder {
    pub(crate) fn new() -> Self {
        //the buffer size was chosen to make sure we can always write any incoming packet
        //without having to handle partial writes.
        //the assertion is valid as long as after every write we attempt to read the kafka message
        //to clear the buffer
        let (write_half, read_half) = tokio::io::duplex(MAX_KAFKA_MESSAGE_SIZE + MAX_PAYLOAD_SIZE);
        Self {
            write_half,
            framed_read_half: FramedRead::new(
                read_half,
                LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_KAFKA_MESSAGE_SIZE)
                    .length_field_length(4)
                    .new_codec(),
            ),
        }
    }

    ///could fail if the length delimiter is bigger than the allowed size
    pub(crate) async fn write_length_encoded(&mut self, payload: Vec<u8>) -> std::io::Result<()> {
        let result = self.write_half.write(&payload).await;
        if let Ok(size) = result {
            if payload.len() == size {
                Ok(())
            } else {
                //should always write the full message, we must fail if this isn't the case
                Err(Error::new(ErrorKind::BrokenPipe, "partial write"))
            }
        } else {
            Err(result.err().unwrap())
        }
    }

    ///returns kafka message decoded from the buffer
    /// if no kafka message is readable with the available buffer None is returned
    pub(crate) async fn read_kafka_message(&mut self) -> Option<Result<BytesMut, Error>> {
        let poll = std::future::poll_fn(|context| {
            match Pin::new(&mut self.framed_read_half).poll_next(context) {
                Poll::Ready(status) => Poll::Ready(status),
                Poll::Pending => Poll::Ready(None),
            }
        });

        poll.await
    }
}
