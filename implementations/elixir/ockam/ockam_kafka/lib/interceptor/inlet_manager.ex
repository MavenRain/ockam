defmodule Ockam.Kafka.Interceptor.InletManager do
  @moduledoc """
  Dynamic inlet manager for kafka interceptor.

  Inlets are GenServers and cannot be managed by ockam registry.
  This module can dynamically create and stop inlets using base port and port offset.
  """
  use GenServer

  def start_link([base_port, allowed_ports, base_route, outlet_prefix]) do
    GenServer.start_link(
      __MODULE__,
      [base_port, allowed_ports, base_route, outlet_prefix],
      name: __MODULE__
    )
  end

  @impl true
  def init([base_port, allowed_ports, base_route, outlet_prefix]) do
    {:ok,
     %{
       base_port: base_port,
       allowed_ports: allowed_ports,
       base_route: base_route,
       outlet_prefix: outlet_prefix,
       inlets: %{}
     }}
  end

  def list_inlets(server \\ __MODULE__, timeout \\ 5000) do
    GenServer.call(server, :list, timeout)
  end

  def create_inlet(server \\ __MODULE__, port_offset, timeout \\ 5000) do
    GenServer.call(server, {:create, port_offset}, timeout)
  end

  def delete_inlet(server \\ __MODULE__, port_offset, timeout \\ 5000) do
    GenServer.call(server, {:delete, port_offset}, timeout)
  end

  @impl true
  def handle_call(:list, _from, %{inlets: inlets} = state) do
    {:reply, inlets, state}
  end

  def handle_call({:create, port_offset}, _from, %{allowed_ports: allowed_ports} = state)
      when port_offset > allowed_ports do
    {:reply, {:error, :port_out_of_range}, state}
  end

  def handle_call({:create, port_offset}, _from, state) do
    case start_inlet(port_offset, state) do
      {:ok, state} ->
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, port_offset}, _from, %{allowed_ports: allowed_ports} = state)
      when port_offset > allowed_ports do
    {:reply, {:error, :port_out_of_range}, state}
  end

  def handle_call({:delete, port_offset}, _from, state) do
    case stop_inlet(port_offset, state) do
      {:ok, state} ->
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp stop_inlet(port_offset, %{inlets: inlets} = state) do
    case Map.fetch(inlets, port_offset) do
      {:ok, pid} ->
        ## FIXME: manage inlets in a supervisor
        try do
          GenServer.stop(pid)
        catch
          :exit, {:noproc, _} ->
            :ok
        end

        %{state | inlets: Map.delete(inlets, port_offset)}

      :error ->
        state
    end
  end

  defp start_inlet(port_offset, %{inlets: inlets} = state) do
    port = inlet_port(port_offset, state)
    peer_route = peer_route(port_offset, state)

    case Ockam.Transport.Portal.InletListener.start_link(port: port, peer_route: peer_route) do
      {:ok, pid} ->
        {:ok, %{state | inlets: Map.put(inlets, port_offset, pid)}}

      {:ok, pid, _extra} ->
        {:ok, %{state | inlets: Map.put(inlets, port_offset, pid)}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp inlet_port(port_offset, state) do
    %{base_port: base_port} = state
    port_offset + base_port
  end

  defp peer_route(port_offset, state) do
    %{base_route: base_route, outlet_prefix: outlet_prefix} = state
    outlet_address = outlet_prefix <> to_string(port_offset)
    base_route ++ [outlet_address]
  end
end
