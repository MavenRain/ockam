use std::str::FromStr;

use clap::Args;
use ockam::Context;
use ockam_api::cloud::lease_manager::models::influxdb::{CreateTokenRequest, CreateTokenResponse};
use ockam_core::api::Request;
use ockam_multiaddr::MultiAddr;

use crate::{
    lease::LeaseArgs,
    util::{node_rpc, orchestrator_api::OrchestratorApiBuilder},
    CommandGlobalOpts,
};

use super::InfluxDbTokenStatus;

/// InfluxDB Token Manager Add On
#[derive(Clone, Debug, Args)]
pub struct InfluxDbCreateCommand {
    /// Optional description of the token
    #[arg(long, id = "description", value_name = "TOKEN_DESCRIPTION")]
    pub description: Option<String>,

    /// Explicitly sets the status of the token
    /// If the token is inactive and requests using the token will be rejected.
    /// Defaults to Active
    #[arg(long, id = "status", value_name = "INFLUXDB_TOKEN_STATUS")]
    pub status: Option<InfluxDbTokenStatus>,

    /// ID of user the authorization is scoped to
    #[arg(long, id = "user_id", value_name = "CLIENT_ID")]
    pub user_id: Option<String>,
}

impl InfluxDbCreateCommand {
    pub fn run(self, opts: CommandGlobalOpts, lease_args: LeaseArgs) {
        node_rpc(run_impl, (opts, lease_args, self));
    }
}

async fn run_impl(
    ctx: Context,
    (opts, lease_args, cmd): (CommandGlobalOpts, LeaseArgs, InfluxDbCreateCommand),
) -> crate::Result<()> {
    let mut orchestrator_client = OrchestratorApiBuilder::new(&ctx, &opts)
        .as_identity(lease_args.cloud_opts.identity)
        .with_new_embbeded_node()
        .await?
        .with_project_from_file(&lease_args.project)
        .await?
        .build(&MultiAddr::from_str("/service")?)
        .await?;

    let body = CreateTokenRequest::new(
        cmd.description,
        cmd.status.map(|s| s.to_string()),
        cmd.user_id,
    );

    let req = Request::post("/lease_manager/influxdb/tokens").body(body);

    let resp: CreateTokenResponse = orchestrator_client.request(req).await?;

    // TODO : Create View for showing created token info
    println!("Token Created: {:?}", resp);

    Ok(())
}
