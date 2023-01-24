use crate::help;
use crate::kafka::producer::ProducerCommand;
use crate::kafka::sidecar::SidecarCommand;
use crate::CommandGlobalOpts;
use clap::{Args, Subcommand};

mod decoder;
mod encoder;
mod kafka_portal_listener;
mod kafka_portal_worker;
mod producer;
mod protocol_aware;
