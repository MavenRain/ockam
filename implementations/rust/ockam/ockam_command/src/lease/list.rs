use std::str::FromStr;

use clap::Args;
use ockam::Context;
use ockam_api::cloud::lease_manager::models::influxdb::Token;
use ockam_core::api::Request;
use ockam_multiaddr::MultiAddr;

use crate::{
    help,
    util::{node_rpc, orchestrator_api::OrchestratorApiBuilder},
    CommandGlobalOpts,
};

use super::LeaseArgs;

const HELP_DETAIL: &str = "";

/// List tokens within the lease token manager
#[derive(Clone, Debug, Args)]
#[command(help_template = help::template(HELP_DETAIL))]
pub struct ListCommand;

impl ListCommand {
    pub fn run(self, options: CommandGlobalOpts, lease_args: LeaseArgs) {
        node_rpc(run_impl, (options, lease_args));
    }
}

async fn run_impl(
    ctx: Context,
    (opts, lease_args): (CommandGlobalOpts, LeaseArgs),
) -> crate::Result<()> {
    let mut orchestrator_client = OrchestratorApiBuilder::new(&ctx, &opts)
        .as_identity(lease_args.cloud_opts.identity)
        .with_new_embbeded_node()
        .await?
        .with_project_from_file(&lease_args.project)
        .await?
        .build(&MultiAddr::from_str("/service/influxdb_token_lease")?)
        .await?;

    let req = Request::get("/");

    let resp_leases: Vec<Token> = orchestrator_client.request_with_response(req).await?;

    // TODO: Create view for listing tokens
    println!("Tokens: \n\n {:?}", resp_leases);

    Ok(())
}
