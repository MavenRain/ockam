use crate::kafka::decoder::KafkaDecoder;
use crate::kafka::encoder::KafkaEncoder;
use crate::kafka::protocol_aware;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use ockam::Context;
use ockam_core::{
    errcode::{Kind, Origin},
    route, Address, Decodable, Error, Processor, Route, Routed, Worker,
};
use ockam_transport_tcp::{PortalMessage, MAX_PAYLOAD_SIZE};
use std::future::{poll_fn, Future};
use std::io::{ErrorKind, Write};
use std::net::SocketAddr;
use std::task::Poll;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::net::tcp::OwnedReadHalf;
use tokio_util::codec;
use tokio_util::codec::{Decoder, Framed, FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::warn;
use tracing::{info, trace};

///by default kafka supports up to 1MB messages
pub(crate) const MAX_KAFKA_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

///can grow over time, up to [MAX_KAFKA_MESSAGE_SIZE]
const INITIAL_KAFKA_BUFFER_SIZE: usize = 16 * 1024;

///the actual limit is 64k but we want a bit of leeway
const MAX_MESSAGE_SIZE: usize = 60 * 1024;

#[derive(Clone, PartialEq, Debug)]
enum State {
    SendPing,
    WaitForPong,
    Initialized,
}

///Acts like a relay for messages between tcp inlet and outlet for both directions.
/// It's meant to be created by the portal listener.
///
/// This instance manage both streams inlet and outlet, since every kafka message
/// is length-delimited every message is read and written through a framed encoder/decoder.
///
/// ```text
/// ┌─────────┐  decoder    ┌─────────┐  encoder     ┌────────┐
/// │   TCP   ├────────────►│  Kafka  ├─────────────►│  TCP   │
/// │  Inlet  │  encoder    │  Portal │  decoder     │ Outlet │
/// │         │◄────────────┤         │◄─────────────┤        │
/// └─────────┘             └─────────┘              └────────┘
///```
pub(crate) struct KafkaPortalWorker {
    inlet_route: Route,
    initial_outlet_route: Route,
    outlet_route: Option<Route>,

    state: State,

    inlet_reader: KafkaDecoder,
    inlet_writer: KafkaEncoder,

    outlet_reader: KafkaDecoder,
    outlet_writer: KafkaEncoder,
}

#[ockam::worker]
impl Worker for KafkaPortalWorker {
    type Message = PortalMessage;
    type Context = Context;

    async fn initialize(&mut self, context: &mut Self::Context) -> ockam::Result<()> {
        assert_eq!(self.state, State::SendPing);
        context
            .send(self.initial_outlet_route.clone(), PortalMessage::Ping)
            .await?;
        self.state = State::WaitForPong;
        Ok(())
    }

    async fn shutdown(&mut self, context: &mut Self::Context) -> ockam::Result<()> {
        let _ = context
            .send(self.inlet_route.clone(), PortalMessage::Disconnect)
            .await;
        if let Some(outlet) = self.outlet_route.clone() {
            let _ = context.send(outlet, PortalMessage::Disconnect).await;
        }
        Ok(())
    }

    async fn handle_message(
        &mut self,
        context: &mut Self::Context,
        message: Routed<Self::Message>,
    ) -> ockam::Result<()> {
        //TODO: create 2 addresses on 1 mailbox and discriminate on them
        //TODO: avoid ping/pong protocol and just being the route
        let source_address = message.msg_addr();
        let outlet_route = message.return_route();
        let portal_message = PortalMessage::decode(message.payload())?;

        match self.state {
            State::SendPing => return self.protocol_error(&portal_message),
            State::WaitForPong => match portal_message {
                PortalMessage::Pong => {
                    self.outlet_route = Some(outlet_route);
                    self.state = State::Initialized;
                }
                _ => {
                    return self.protocol_error(&portal_message);
                }
            },
            State::Initialized => {
                match portal_message {
                    PortalMessage::Payload(message) => {
                        assert!(self.outlet_route.is_some());
                        let outlet_route = self.outlet_route.clone().unwrap();

                        if source_address == self.inlet_route.recipient() {
                            let result = Self::decode_and_transform_message(
                                &mut self.inlet_reader,
                                &mut self.outlet_writer,
                                message,
                            )
                            .await;
                            if let Err(cause) = result {
                                return Err(Error::new(Origin::Transport, Kind::Io, cause));
                            }

                            if let Some(encoded_message) = result.unwrap() {
                                self.split_and_send(context, outlet_route, encoded_message)
                                    .await?;
                            }
                        } else if source_address == outlet_route.recipient() {
                            let result = Self::decode_and_transform_message(
                                &mut self.outlet_reader,
                                &mut self.inlet_writer,
                                message,
                            )
                            .await;
                            if let Err(cause) = result {
                                return Err(Error::new(Origin::Transport, Kind::Io, cause));
                            }

                            if let Some(encoded_message) = result.unwrap() {
                                self.split_and_send(
                                    context,
                                    self.inlet_route.clone(),
                                    encoded_message,
                                )
                                .await?;
                            }
                        } else {
                            //message received from unknown party
                            return Err(Error::new(
                                Origin::Transport,
                                Kind::Protocol,
                                "invalid source address",
                            ));
                        }
                    }

                    PortalMessage::Disconnect => {
                        //the shutdown will take care of sending disconnect
                        //TODO: await == deadlock?
                        context.stop_worker(context.address()).await?
                    }
                    PortalMessage::Ping | PortalMessage::Pong => {
                        //should never receive a ping since the listener already received it
                        return self.protocol_error(&portal_message);
                    }
                }
            }
        }

        Ok(())
    }
}

impl KafkaPortalWorker {
    fn protocol_error(&self, message: &PortalMessage) -> Result<(), Error> {
        warn!(
            "protocol error: unexpected message; message = {:?}; state = {:?};",
            message, self.state
        );
        Err(Error::new(
            Origin::Transport,
            Kind::Protocol,
            "invalid message",
        ))
    }

    pub(crate) async fn split_and_send(
        &self,
        context: &mut Context,
        route: Route,
        mut buffer: Vec<u8>,
    ) -> ockam_core::Result<()> {
        for chunk in buffer.chunks(//TODO: portal max size) {
            context
                .send(route.clone(), PortalMessage::Payload(chunk.to_vec()))
                .await?;
        }
        Ok(())
    }

    async fn decode_and_transform_message(
        reader: &mut KafkaDecoder,
        writer: &mut KafkaEncoder,
        encoded_message: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        reader.write_length_encoded(encoded_message).await?;

        let maybe_result = reader.read_kafka_message().await;
        if maybe_result.is_none() {
            return Ok(None);
        }

        let complete_kafka_message = maybe_result.unwrap()?;
        let transformed_message = protocol_aware::transform_if_necessary(complete_kafka_message);
        writer
            .write_kafka_message(transformed_message.to_vec())
            .await?;

        Ok(Some(writer.read_length_encoded().await?))
    }
}

impl KafkaPortalWorker {
    pub(crate) fn new(
        inlet_route: impl Into<Route>,
        ping_outlet_route: impl Into<Route>,
    ) -> KafkaPortalWorker {
        Self {
            inlet_route: inlet_route.into(),
            initial_outlet_route: ping_outlet_route.into(),
            outlet_route: None,
            state: State::SendPing,

            inlet_reader: KafkaDecoder::new(),
            inlet_writer: KafkaEncoder::new(),

            outlet_reader: KafkaDecoder::new(),
            outlet_writer: KafkaEncoder::new(),
        }
    }
}
