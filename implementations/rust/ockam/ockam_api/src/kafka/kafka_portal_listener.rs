use crate::kafka::kafka_portal_worker::KafkaPortalWorker;
use ockam::Context;
use ockam_core::{Address, AllowAll, Any, Routed, Worker};
use tracing::trace;
use tracing::warn;

///Acts like an outlet listener of tcp transport
/// only receives first ping and spawn a dedicated worker to handle the rest
pub(crate) struct KafkaPortalListener;

#[ockam::worker]
impl Worker for KafkaPortalListener {
    type Message = Any;
    type Context = Context;

    async fn handle_message(
        &mut self,
        context: &mut Self::Context,
        message: Routed<Self::Message>,
    ) -> ockam::Result<()> {
        trace!("received message: {:?}", &message);

        let result = KafkaPortalWorker::start(context).await;
        if let Err(err) = result {
            warn!("cannot spawn worker; err = {:?}", err);
            return Err(err);
        }
        let worker_address = result.unwrap();

        //forward to the worker and place its address in the return route
        let mut message = message.into_local_message();

        message
            .transport_mut()
            .onward_route
            .modify()
            .replace(worker_address.clone());

        trace!(
            "forwarding message: onward={:?}; return={:?}; worker={:?}",
            &message.transport().onward_route,
            &message.transport().return_route,
            worker_address
        );

        context.forward(message).await?;

        Ok(())
    }
}

impl KafkaPortalListener {
    pub(crate) async fn start(context: &Context, address: Address) -> ockam_core::Result<()> {
        //TODO: only allow the inlet
        context
            .start_worker(address, Self {}, AllowAll, AllowAll)
            .await
    }
}
