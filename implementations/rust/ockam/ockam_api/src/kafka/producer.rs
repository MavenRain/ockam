use crate::kafka::kafka_portal_listener::KafkaPortalListener;
use crate::nodes::models::services::StartKafkaProducerRequest;
use crate::util::node_rpc;
use crate::{help, CommandGlobalOpts};
use clap::{Args, Subcommand};
use ockam::access_control::AllowAll;
use ockam::{Context, Result, TcpTransport};
use ockam_core::{Address, Route};
use std::time::Duration;
use tracing::{info, warn};

struct Producer {}

impl Producer {
    pub async fn run(context: Context, request: StartKafkaProducerRequest) -> crate::Result<()> {
        //needed to send messages to the orchestrator
        let _tcp = TcpTransport::create(&context).await?;

        KafkaPortalListener::start(&context, cmd.from, cmd.destination).await?;
        info!("kafka listener started");

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
