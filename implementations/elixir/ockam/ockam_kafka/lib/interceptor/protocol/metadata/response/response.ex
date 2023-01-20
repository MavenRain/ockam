defmodule Ockam.Kafka.Interceptor.Protocol.Metadata.Response do
  defstruct [
    :api_version,
    :throttle_time_ms,
    :brokers,
    :cluster_id,
    :controller_id,
    :topics,
    :cluster_authorized_operations,
    :tagged_fields
  ]

  defmodule Topic do
    defstruct [
      :error_code,
      :name,
      :topic_id,
      :is_internal,
      :partitions,
      :tagged_fields,
      :topic_authorized_operations
    ]

    defmodule Partition do
      defstruct [
        :error_code,
        :partition_index,
        :leader_id,
        :leader_epoch,
        :replica_nodes,
        :isr_nodes,
        :offline_replicas,
        :tagged_fields
      ]
    end
  end

  defmodule Broker do
    defstruct [
      :node_id,
      :host,
      :port,
      :rack,
      :tagged_fields
    ]
  end
end
