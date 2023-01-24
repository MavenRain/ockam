use crate::nodes::models::policy::{Policy, PolicyList};
use crate::nodes::NodeManagerWorker;
use minicbor::{Decode, Decoder, Encode};
use ockam_core::api::{Error, Request, Response, ResponseBuilder};
use ockam_core::{CowStr, Result, Route, TypeTag};
use ockam_node::Context;

use super::NodeManager;

#[derive(Encode, Decode, Debug)]
#[cfg_attr(test, derive(Clone))]
#[rustfmt::skip]
#[cbor(map)]
pub struct StartKafkaProducerSidecar {
    #[cfg(feature = "tag")]
    #[serde(skip)]
    #[n(0)]
    pub tag: TypeTag<6605232>,
    // #[b(1)]
    // pub id: Route,
}

impl NodeManagerWorker {
    pub(crate) async fn create_kafka_producer_sidecar(
        &self,
        ctx: &mut Context,
        req: &Request<'_>,
        dec: &mut Decoder<'_>,
    ) -> Result<Vec<u8>> {
        todo!()
    }
}
