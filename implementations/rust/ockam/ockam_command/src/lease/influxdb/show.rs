use std::str::FromStr;

use clap::Args;
use ockam::Context;
use ockam_api::cloud::lease_manager::models::influxdb::{ShowTokenRequest, ShowTokenResponse};

use ockam_core::api::Request;
use ockam_multiaddr::MultiAddr;

use crate::{
    lease::LeaseArgs,
    util::{node_rpc, orchestrator_api::OrchestratorApiBuilder},
    CommandGlobalOpts,
};

/// InfluxDB Token Manager Add On
#[derive(Clone, Debug, Args)]
pub struct InfluxDbShowCommand {
    /// ID of the token to retrieve
    #[arg(short, long, value_name = "INFLUX_DB_TOKEN_ID")]
    pub token_id: String,
}

impl InfluxDbShowCommand {
    pub fn run(self, opts: CommandGlobalOpts, lease_args: LeaseArgs) {
        node_rpc(run_impl, (opts, lease_args, self));
    }
}

async fn run_impl(
    ctx: Context,
    (opts, lease_args, cmd): (CommandGlobalOpts, LeaseArgs, InfluxDbShowCommand),
) -> crate::Result<()> {
    let mut orchestrator_client = OrchestratorApiBuilder::new(&ctx, &opts)
        .as_identity(lease_args.cloud_opts.identity)
        .with_new_embbeded_node()
        .await?
        .with_project_from_file(&lease_args.project)
        .await?
        .build(&MultiAddr::from_str("/service/influxdb_token_lease")?)
        .await?;
    let body = ShowTokenRequest::new(cmd.token_id.clone());

    let req = Request::get(format!("/{}", cmd.token_id)).body(body);

    let resp: ShowTokenResponse = orchestrator_client.request(req).await?;

    // TODO: Create view for showing a token
    println!("Token details: {:?}", resp);
    Ok(())
}
