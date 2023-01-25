defmodule Ockam.Transport.Portal.Interceptor do
  @moduledoc """
  Interceptor worker for portals.

  Can be inserted in the route between inlet and outlet.
  Messages on outer address are coming from the inlet, messages on inner address
  are coming from the outlet.

  Supports `:init_message` handling from `Ockam.Session.Spawner`

  Messages are parsed and processed by behaviour functions
  For payload messages:
  `handle_outer_payload`
  `handle_inner_payload`

  For signal messages (:ping, :pong, :disconnect):
  `handle_outer_signal`
  `handle_inner_signal`

  :disconnect stops the interceptor worker

  Other messages are forwarded as is.

  `outer` and `inner` depends on which direction message flow created the interceptor,
  usually it's configured in the route of the inlet, so `outer` is inlet and `inner` is outlet.

  Options:
  :interceptor_mod - module implementing interceptor behaviour
  :interceptor_options - options to initialize interceptor with
  :init_message - message from `Ockam.Session.Spawner`
  """
  use Ockam.AsymmetricWorker

  alias Ockam.Message
  alias Ockam.Router
  alias Ockam.Transport.Portal.TunnelProtocol

  @doc """
  Modify interceptor worker state.
  """
  @callback setup(options :: Keyword.t(), state :: map()) ::
              {:ok, state :: map()} | {:error, reason :: any()}

  @doc """
  Process intercepted payload from outer worker.
  Returns:
   - {:ok, state} - will forward original payload
   - {:ok, payload, state} - will replace intercepted payload
   - {:error, reason} - will not forward the payload
  """
  @callback handle_outer_payload(payload :: binary(), state :: map()) ::
              {:ok, state :: map()}
              | {:ok, payload :: binary(), state :: map()}
              | {:error, reason :: any()}
  @doc """
  Process intercepted payload from inner worker.
  Returns:
   - {:ok, state} - will forward original payload
   - {:ok, payload, state} - will replace intercepted payload
   - {:error, reason} - will not forward the payload
  """
  @callback handle_inner_payload(payload :: binary(), state :: map()) ::
              {:ok, state :: map()}
              | {:ok, payload :: binary(), state :: map()}
              | {:error, reason :: any()}
  @doc """
  Process intercepted signal from outer worker (:ping, :pong, :disconnect).
  Returns:
   - {:ok, state} - will forward original signal
   - {:error, reason} - will not forward the signal
  """
  @callback handle_outer_signal(signal :: any(), state :: map()) ::
              {:ok, state :: map()} | {:error, reason :: any()}
  @doc """
  Process intercepted signal from inner worker (:ping, :pong, :disconnect).
  Returns:
   - {:ok, state} - will forward original signal
   - {:error, reason} - will not forward the signal
  """
  @callback handle_inner_signal(signal :: any(), state :: map()) ::
              {:ok, state :: map()} | {:error, reason :: any()}

  @impl true
  def inner_setup(options, state) do
    interceptor_mod = Keyword.fetch!(options, :interceptor_mod)
    interceptor_options = Keyword.get(options, :interceptor_options, [])

    case interceptor_mod.setup(interceptor_options, state) do
      {:ok, state} ->
        case Keyword.fetch(options, :init_message) do
          {:ok, message} ->
            ## Interceptor is spawned by Ockam.Session.Spawner
            ## init message is forwarded
            Message.forward(message)
            |> Message.trace(state.inner_address)
            |> Router.route()

          :error ->
            :ok
        end

        {:ok, Map.put(state, :interceptor_mod, interceptor_mod)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def handle_outer_message(%Message{payload: payload} = message, state) do
    with {:ok, new_payload, state} <- handle_tunnel_message(:outer, payload, state) do
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
    with {:ok, new_payload, state} <- handle_tunnel_message(:inner, payload, state) do
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
        ## FIXME: stop on :disconnect signal
        case handle_signal(type, signal, state) do
          {:ok, state} ->
            {:ok, payload, state}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        ## TODO: should we return error here?
        Logger.warn("Cannot parse tunnel message #{inspect(reason)}")
        {:ok, payload, state}
    end
  end

  def handle_payload(type, data, %{interceptor_mod: interceptor_mod} = state) do
    case type do
      :outer -> interceptor_mod.handle_outer_payload(data, state)
      :inner -> interceptor_mod.handle_inner_payload(data, state)
    end
  end

  def handle_signal(type, data, %{interceptor_mod: interceptor_mod} = state) do
    case type do
      :outer -> interceptor_mod.handle_outer_signal(data, state)
      :inner -> interceptor_mod.handle_inner_signal(data, state)
    end
  end
end
