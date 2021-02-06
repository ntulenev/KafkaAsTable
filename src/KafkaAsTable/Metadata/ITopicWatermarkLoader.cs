using System;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaAsTable.Watermarks;

namespace KafkaAsTable.Metadata
{
    public interface ITopicWatermarkLoader
    {
        public Task<TopicWatermark> LoadWatermarksAsync<Key, Value>(Func<IConsumer<Key, Value>> consumerFactory, CancellationToken ct);
    }
}
