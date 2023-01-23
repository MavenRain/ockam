defmodule Ockam.Kafka.Interceptor.MetadataHandler do
  @moduledoc """
  Metadata response handlers for kafka interceptor.

  Support creation of inlets and outlets for metadata brokers
  """
  alias Ockam.Kafka.Interceptor.Protocol.ResponseHeader

  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Response, as: MetadataResponse
  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Response.Broker

  alias Ockam.Kafka.Interceptor.InletManager

  require Logger

  def outlet_response(%ResponseHeader{}, %MetadataResponse{} = response, state) do
    case create_broker_outlets(response, state) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end
  def outlet_response(%ResponseHeader{api_key: api_key}, _response, state) do
    Logger.info("Ignoring response with api key #{inspect(api_key)}")
    {:ok, state}
  end

  def inlet_response(%ResponseHeader{}, %MetadataResponse{} = response, state) do
    base_port = Map.fetch!(state, :base_port)
    case create_broker_inlets(response, base_port) do
      {:ok, new_response} ->
        {:ok, new_response, state}
      {:error, reason} ->
        {:error, reason}
    end
  end
  def inlet_response(%ResponseHeader{api_key: api_key}, _response, state) do
    Logger.info("Ignoring response with api key #{inspect(api_key)}")
    {:ok, state}
  end

  def create_broker_inlets(%MetadataResponse{brokers: brokers} = response, base_port) do
    metadata_inlet_nodes = Enum.map(brokers, fn(broker) -> broker.node_id end) |> Enum.sort()

    existing_inlet_nodes = InletManager.list_inlets() |> Enum.map(fn({node_id, _pid}) -> node_id end) |> Enum.sort()

    update_inlets(existing_inlet_nodes, metadata_inlet_nodes)

    brokers = Enum.map(brokers, fn(%Broker{node_id: node_id} = broker) ->
      %{broker | host: "localhost", port: inlet_port(node_id, base_port)}
    end)
    {:ok, %{response | brokers: brokers}}
  end

  def create_broker_outlets(%MetadataResponse{brokers: brokers}, state) do
    metadata_outlets =
      Enum.map(brokers, fn broker ->
        {to_string(broker.node_id), {broker.host, broker.port}}
      end)
      |> Map.new()

    outlet_prefix = Map.fetch!(state, :outlet_prefix)

    existing_outlets =
      Ockam.Node.list_addresses()
      |> Enum.filter(fn address -> String.starts_with?(address, outlet_prefix) end)
      |> Enum.map(fn address ->
        ## TODO: explicit API to fetch worker options from outlet
        Ockam.Node.whereis(address) |> :sys.get_state() |> Map.take([:address, :worker_options])
      end)
      |> Enum.map(fn %{address: address, worker_options: options} ->
        target_host = Keyword.fetch!(options, :target_host)
        target_port = Keyword.fetch!(options, :target_port)

        {
          String.replace_prefix(address, outlet_prefix, ""),
          {target_host, target_port}
        }
      end)
      |> Map.new()

    case metadata_outlets == existing_outlets do
      true ->
        :ok

      false ->
        update_outlets(existing_outlets, metadata_outlets, outlet_prefix)
    end
  end

  def update_inlets(existing_inlets, metadata_inlets) do
    extra = existing_inlets -- metadata_inlets

    Enum.each(extra, fn(node_id) ->
      :ok = InletManager.delete_inlet(node_id)
    end)

    missing = metadata_inlets -- existing_inlets

    Enum.each(missing, fn(node_id) ->
      :ok = InletManager.create_inlet(node_id)
    end)
  end

  ## TODO: maybe we shold return the port from InletManager functions
  def inlet_port(node_id, base_port) do
    base_port + node_id
  end

  def outlet_address(node_id, outlet_prefix) do
    outlet_prefix <> to_string(node_id)
  end

  ## TODO: maybe we want to terminate existing connections when outlets are reshuffled??
  def update_outlets(existing_outlets, metadata_outlets, outlet_prefix) do
    ## FIXME: there could be concurrent requests to metadata.
    ## We should make the outlet reconciliation synchronized in gen_server

    ## Terminate all old outlets and start new ones.
    ## TODO: we could skip outlets with same config
    Enum.each(existing_outlets, fn {node_id, _} ->
      stop_outlet(node_id, outlet_prefix)
    end)

    Enum.each(metadata_outlets, fn {node_id, {target_host, target_port}} ->
      start_outlet(node_id, target_host, target_port, outlet_prefix)
    end)

    :ok
  end

  def stop_outlet(node_id, outlet_prefix) do
    Ockam.Node.stop(outlet_address(node_id, outlet_prefix))
  end

  def start_outlet(node_id, target_host, target_port, outlet_prefix) do
    address = outlet_address(node_id, outlet_prefix)
    ## FIXME: handle failures
    Ockam.Session.Spawner.create(
      address: address,
      worker_mod: Ockam.Transport.Portal.OutletWorker,
      worker_options: [target_host: target_host, target_port: target_port]
    )
  end
end
