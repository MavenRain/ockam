defmodule Ockam.Transport.Portal.Interceptor do
  use Ockam.AsymmetricWorker

  alias Ockam.Message
  alias Ockam.Router
  alias Ockam.Transport.Portal.TunnelProtocol

  @doc """
  Modify interceptor worker state.
  """
  @callback setup(options::Keyword.t, state::map()) :: {:ok, state::map()} | {:error, reason::any()}

  @doc """
  Process intercepted payload from inlet.
  Returns:
   - {:ok, state} - will forward original payload
   - {:ok, payload, state} - will replace intercepted payload
   - {:error, reason} - will not forward the payload
  """
  @callback handle_inlet_payload(payload::binary(), state::map()) :: {:ok, state::map()} | {:ok, payload::binary(), state::map()} | {:error, reason::any()}
  @doc """
  Process intercepted payload from outlet.
  Returns:
   - {:ok, state} - will forward original payload
   - {:ok, payload, state} - will replace intercepted payload
   - {:error, reason} - will not forward the payload
  """
  @callback handle_outlet_payload(payload::binary(), state::map()) :: {:ok, state::map()} | {:ok, payload::binary(), state::map()} | {:error, reason::any()}
  @doc """
  Process intercepted signal from inlet (:ping, :pong, :disconnect).
  Returns:
   - {:ok, state} - will forward original signal
   - {:error, reason} - will not forward the signal
  """
  @callback handle_inlet_signal(signal::any(), state::map()) :: {:ok, state::map()} | {:error, reason::any()}
  @doc """
  Process intercepted signal from outlet (:ping, :pong, :disconnect).
  Returns:
   - {:ok, state} - will forward original signal
   - {:error, reason} - will not forward the signal
  """
  @callback handle_outlet_signal(signal::any(), state::map()) :: {:ok, state::map()} | {:error, reason::any()}

  @impl true
  def inner_setup(options, state) do
    interceptor_mod = Keyword.fetch!(options, :interceptor_mod)
    case interceptor_mod.setup(options, state) do
      {:ok, state} ->
        {:ok, Map.put(state, :interceptor_mod, interceptor_mod)}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def handle_outer_message(%Message{payload: payload} = message, state) do
    with {:ok, new_payload, state} <- handle_tunnel_message(:inlet, payload, state) do
      new_message =
        message
        |> Message.forward()
        |> Message.trace(state.inner_address)
        |> Message.set_payload(new_payload)

      Router.route(new_message)
      {:ok, state}
    end
  end

  @impl true
  def handle_inner_message(%Message{payload: payload} = message, state) do
    with {:ok, new_payload, state} <- handle_tunnel_message(:outlet, payload, state) do
      new_message =
        message
        |> Message.forward()
        |> Message.trace(state.address)
        |> Message.set_payload(new_payload)

      Router.route(new_message)
      {:ok, state}
    end
  end

  def handle_tunnel_message(type, payload, state) do
    case TunnelProtocol.decode(payload) do
      {:ok, {:payload, data}} ->
        case handle_payload(type, data, state) do
          {:ok, state} ->
            {:ok, payload, state}

          {:ok, updated_data, state} ->
            {:ok, TunnelProtocol.encode({:payload, updated_data}), state}

          {:error, reason} ->
            {:error, reason}
        end

      {:ok, signal} ->
        case handle_signal(type, signal, state) do
          {:ok, state} ->
            {:ok, payload, state}
          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        ## FIXME: should we return error here?
        Logger.warn("Cannot parse tunnel message #{inspect(reason)}")
        {:ok, payload, state}
    end
  end

  def handle_payload(type, data, %{interceptor_mod: interceptor_mod} = state) do
    case type do
      :inlet -> interceptor_mod.handle_inlet_payload(data, state)
      :outlet -> interceptor_mod.handle_outlet_payload(data, state)
    end
  end
  def handle_signal(type, data, %{interceptor_mod: interceptor_mod} = state) do
    case type do
      :inlet -> interceptor_mod.handle_inlet_signal(data, state)
      :outlet -> interceptor_mod.handle_outlet_signal(data, state)
    end
  end
end
