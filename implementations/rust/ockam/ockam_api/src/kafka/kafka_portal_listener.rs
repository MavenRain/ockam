use crate::kafka::kafka_portal_worker::KafkaPortalWorker;
use crate::kafka::protocol_aware;
use bytes::{Buf, BufMut, BytesMut};
use futures::stream::StreamExt;
use ockam::Context;
use ockam_core::{
    errcode::{Kind, Origin},
    route, Address, AllowAll, Decodable, DenyAll, Error, Processor, Route, Routed, Worker,
};
use ockam_transport_tcp::PortalMessage;
use std::io::Write;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio_util::codec;
use tokio_util::codec::{Decoder, Framed, FramedRead, LengthDelimitedCodec};
use tracing::warn;
use tracing::{info, trace};

///Acts like an outlet listener of tcp transport
/// only receives first ping and spawn a dedicated worker to handle the rest
pub(crate) struct KafkaPortalListener {
    initial_outlet_route: Route,
}

#[ockam::worker]
impl Worker for KafkaPortalListener {
    type Message = PortalMessage;
    type Context = Context;

    async fn handle_message(
        &mut self,
        context: &mut Self::Context,
        message: Routed<Self::Message>,
    ) -> ockam::Result<()> {
        let inlet_route = message.return_route();
        let message = PortalMessage::decode(message.payload())?;
        match message {
            PortalMessage::Ping => {
                //TODO: restrict access
                // i know the address of the inlet
                // but i don't know the final address of the outlet yet
                let result = context
                    .start_worker(
                        Address::random_local(),
                        KafkaPortalWorker::new(inlet_route, self.initial_outlet_route.clone()),
                        AllowAll,
                        AllowAll,
                    )
                    .await;
                if let Err(err) = result {
                    warn!("cannot spawn worker; err = {:?}", err);
                    return Err(err);
                }
                Ok(())
            }
            _ => {
                warn!(
                    "invalid message in kafka portal listener; message = {:?}",
                    message
                );
                Err(Error::new(
                    Origin::Transport,
                    Kind::Protocol,
                    "invalid message",
                ))
            }
        }
    }
}

impl KafkaPortalListener {
    pub(crate) async fn start(
        context: &Context,
        address: Address,
        outlet_route: impl Into<Route>,
    ) -> ockam_core::Result<()> {
        //allow any inlet, deny any reply since the worker should be the one replying to the ping
        context
            .start_worker(
                address,
                Self {
                    initial_outlet_route: outlet_route.into(),
                },
                AllowAll,
                DenyAll,
            )
            .await
    }
}
