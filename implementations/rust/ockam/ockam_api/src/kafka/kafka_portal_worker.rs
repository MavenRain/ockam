use crate::kafka::decoder::KafkaDecoder;
use crate::kafka::encoder::KafkaEncoder;
use crate::kafka::protocol_aware;
use ockam::Context;
use ockam_core::{
    errcode::{Kind, Origin},
    Address, AllowAll, Decodable, Encodable, Error, LocalInfo, LocalMessage, Route, Routed,
    TransportMessage, Worker,
};
use ockam_transport_tcp::{PortalMessage, MAX_PAYLOAD_SIZE};

///by default kafka supports up to 1MB messages
pub(crate) const MAX_KAFKA_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

enum Receiving {
    Requests,
    Responses,
}

///Acts like a relay for messages between tcp inlet and outlet for both directions.
/// It's meant to be created by the portal listener.
///
/// This instance manage both streams inlet and outlet in two different workers, one dedicated
/// to the requests (inlet=>outlet) the other for the responses (outlet=>inlet).
/// since every kafka message is length-delimited every message is read and written
/// through a framed encoder/decoder.
///
/// ```text
/// ┌────────┐  decoder    ┌─────────┐  encoder    ┌────────┐
/// │        ├────────────►│ Kafka   ├────────────►│        │
/// │        │             │ Request │             │        │
/// │  TCP   │             └─────────┘             │  TCP   │
/// │ Inlet  │             ┌─────────┐             │ Outlet │
/// │        │  encoder    │ Kafka   │   decoder   │        │
/// │        │◄────────────┤ Response│◄────────────┤        │
/// └────────┘             └─────────┘             └────────┘
///```
pub(crate) struct KafkaPortalWorker {
    other_worker_address: Address,
    reader: KafkaDecoder,
    writer: KafkaEncoder,
    receiving: Receiving,
}

#[ockam::worker]
impl Worker for KafkaPortalWorker {
    type Message = PortalMessage;
    type Context = Context;

    async fn handle_message(
        &mut self,
        context: &mut Self::Context,
        routed_message: Routed<Self::Message>,
    ) -> ockam::Result<()> {
        trace!("received message: {:?}", &routed_message);

        let onward_route = routed_message.onward_route();
        let return_route = routed_message.return_route();
        let portal_message = PortalMessage::decode(routed_message.payload())?;

        match portal_message {
            PortalMessage::Payload(message) => {
                let result = self.decode_and_transform_messages(message).await;
                if let Err(cause) = result {
                    return Err(Error::new(Origin::Transport, Kind::Io, cause));
                }

                if let Some(encoded_message) = result.unwrap() {
                    self.split_and_send(
                        context,
                        onward_route,
                        return_route,
                        encoded_message,
                        routed_message.local_message().local_info(),
                    )
                    .await?;
                }
            }
            PortalMessage::Disconnect => {
                //the shutdown will take care of sending disconnect
                context
                    .stop_worker(self.other_worker_address.clone())
                    .await?;
                context.stop_worker(context.address()).await?;
                self.forward(context, routed_message).await?;
            }
            _ => self.forward(context, routed_message).await?,
        }

        Ok(())
    }
}

impl KafkaPortalWorker {
    async fn forward(
        &self,
        context: &mut Context,
        routed_message: Routed<PortalMessage>,
    ) -> ockam_core::Result<()> {
        trace!(
            "before: onwards={:?}; return={:?};",
            routed_message.local_message().transport().onward_route,
            routed_message.local_message().transport().return_route
        );
        //to correctly proxy messages to the inlet or outlet side
        //we invert the return route when a message pass through
        let mut local_message = routed_message.into_local_message();
        let transport = local_message.transport_mut();
        transport
            .return_route
            .modify()
            .prepend(self.other_worker_address.clone());

        transport.onward_route.step()?;

        trace!(
            "after: onwards={:?}; return={:?};",
            local_message.transport().onward_route,
            local_message.transport().return_route
        );
        context.forward(local_message).await
    }

    async fn split_and_send(
        &self,
        context: &mut Context,
        onward_route: Route,
        return_route: Route,
        buffer: Vec<u8>,
        local_info: &[LocalInfo],
    ) -> ockam_core::Result<()> {
        for chunk in buffer.chunks(MAX_PAYLOAD_SIZE) {
            //to correctly proxy messages to the inlet or outlet side
            //we invert the return route when a message pass through
            let message = LocalMessage::new(
                TransportMessage::v1(
                    onward_route.clone().modify().pop_front(),
                    return_route
                        .clone()
                        .modify()
                        .prepend(self.other_worker_address.clone()),
                    PortalMessage::Payload(chunk.to_vec()).encode()?,
                ),
                local_info.to_vec(),
            );

            trace!(
                "sending message: onward={:?}; return={:?};",
                &message.transport().onward_route,
                &message.transport().return_route
            );

            context.forward(message).await?;
        }
        Ok(())
    }

    async fn decode_and_transform_messages(
        &mut self,
        encoded_message: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        self.reader.write_length_encoded(encoded_message).await?;

        loop {
            let maybe_result = self.reader.read_kafka_message().await;
            if maybe_result.is_none() {
                break;
            }

            let complete_kafka_message = maybe_result.unwrap()?;
            let transformed_message = match self.receiving {
                Receiving::Requests => protocol_aware::peek_requests(complete_kafka_message),
                Receiving::Responses => complete_kafka_message,
            };

            self.writer
                .write_kafka_message(transformed_message.to_vec())
                .await?;
        }

        let length_encoded_buffer = self.writer.read_length_encoded().await?;
        Ok(Some(length_encoded_buffer))
    }
}

impl KafkaPortalWorker {
    ///returns address used for inlet communications
    pub(crate) async fn start(context: &mut Context) -> ockam_core::Result<Address> {
        let inlet_address = Address::random_local();
        let outlet_address = Address::random_local();

        let inlet_worker = Self {
            other_worker_address: outlet_address.clone(),
            reader: KafkaDecoder::new(),
            writer: KafkaEncoder::new(),
            receiving: Receiving::Requests,
        };
        let outlet_worker = Self {
            other_worker_address: inlet_address.clone(),
            reader: KafkaDecoder::new(),
            writer: KafkaEncoder::new(),
            receiving: Receiving::Responses,
        };

        context
            .start_worker(inlet_address.clone(), inlet_worker, AllowAll, AllowAll)
            .await?;

        context
            .start_worker(outlet_address, outlet_worker, AllowAll, AllowAll)
            .await?;

        Ok(inlet_address)
    }
}

#[cfg(test)]
mod test {
    use crate::kafka::kafka_portal_worker::KafkaPortalWorker;
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, RequestHeader};
    use kafka_protocol::protocol::Encodable as KafkaEncodable;
    use kafka_protocol::protocol::StrBytes;
    use ockam::Context;
    use ockam_core::{route, Routed};
    use ockam_transport_tcp::PortalMessage;

    #[allow(non_snake_case)]
    #[ockam_macros::test(timeout = 5000)]
    async fn kafka_portal_worker__ping_pong_pass_through__should_pass(
        context: &mut Context,
    ) -> ockam::Result<()> {
        let portal_inlet_address = KafkaPortalWorker::start(context).await?;

        context
            .send(
                route![portal_inlet_address, context.address()],
                PortalMessage::Ping,
            )
            .await?;

        let message: Routed<PortalMessage> = context.receive::<PortalMessage>().await?.take();
        if let PortalMessage::Ping = message.as_body() {
        } else {
            assert!(false, "invalid message type")
        }

        context
            .send(message.return_route(), PortalMessage::Pong)
            .await?;

        let message: Routed<PortalMessage> = context.receive::<PortalMessage>().await?.take();
        if let PortalMessage::Pong = message.as_body() {
        } else {
            assert!(false, "invalid message type")
        }

        context.stop().await
    }

    #[allow(non_snake_case)]
    #[ockam_macros::test(timeout = 5000)]
    async fn kafka_portal_worker__kafka_messages_pass_through__should_pass(
        context: &mut Context,
    ) -> ockam::Result<()> {
        let mut buffer = BytesMut::new();

        //let's create a real kafka request and pass it through the portal
        let request_header = RequestHeader {
            request_api_key: ApiKey::ApiVersionsKey as i16,
            request_api_version: 4,
            correlation_id: 1,
            client_id: Some(StrBytes::from_str("my-id")),
            unknown_tagged_fields: Default::default(),
        };
        let api_versions_request = ApiVersionsRequest {
            client_software_version: StrBytes::from_str("1.0"),
            client_software_name: StrBytes::from_str("test-client"),
            unknown_tagged_fields: Default::default(),
        };

        let size =
            request_header.compute_size(2).unwrap() + api_versions_request.compute_size(3).unwrap();
        buffer.put_u32(size as u32);

        request_header.encode(&mut buffer, 2).unwrap();
        api_versions_request.encode(&mut buffer, 3).unwrap();

        trace!("sizes: kafka={}; encoded={};", size, buffer.len());

        let portal_inlet_address = KafkaPortalWorker::start(context).await?;

        context
            .send(
                route![portal_inlet_address, context.address()],
                PortalMessage::Payload(buffer.to_vec()),
            )
            .await?;

        let message: Routed<PortalMessage> = context.receive::<PortalMessage>().await?.take();

        if let PortalMessage::Payload(payload) = message.as_body() {
            assert_eq!(&buffer.to_vec(), payload);
        } else {
            assert!(false, "invalid message type")
        }

        //TODO: answer using kafka response

        context.stop().await
    }
}
