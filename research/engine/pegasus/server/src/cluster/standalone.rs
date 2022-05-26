use crate::job::JobAssembly;
use crate::rpc::{RPCServerConfig, ServiceStartListener};
use std::net::SocketAddr;

struct StandaloneServiceListener;

impl ServiceStartListener for StandaloneServiceListener {
    fn on_rpc_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("RPC server of server[{}] start on {}", server_id, addr);
        Ok(())
    }

    fn on_server_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("compute server[{}] start on {}", server_id, addr);
        Ok(())
    }
}

pub async fn start<P>(
    rpc_config: RPCServerConfig, server_config: pegasus::Configuration, assemble: P, blocking: bool,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: JobAssembly,
{
    let detect = if let Some(net_conf) = server_config.network_config() {
        net_conf.get_servers()?.unwrap_or(vec![])
    } else {
        vec![]
    };

    crate::rpc::start_all_servers(
        rpc_config,
        server_config,
        assemble,
        detect,
        &mut StandaloneServiceListener,
        blocking,
    )
    .await?;
    Ok(())
}
