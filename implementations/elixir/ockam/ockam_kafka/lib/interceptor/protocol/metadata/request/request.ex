defmodule Ockam.Kafka.Interceptor.Protocol.Metadata.Request do
  defstruct [
    :api_version,
    :topics,
    :allow_auto_topic_creation,
    :include_cluster_authorized_operations,
    :include_topic_authorized_operations,
    :tagged_fields
  ]

  defmodule Topic do
    defstruct [
      :name,
      :topic_id,
      :tagged_fields
    ]
  end
end
