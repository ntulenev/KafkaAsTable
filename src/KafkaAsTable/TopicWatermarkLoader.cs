using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaAsTable.Helpers;
using KafkaAsTable.Model;

namespace KafkaAsTable
{
    public class TopicWatermarkLoader : ITopicWatermarkLoader
    {
        public TopicWatermarkLoader(string topicName,
                                    IAdminClient adminClient,
                                    int intTimeoutSeconds)
        {
            KafkaValidationHelper.ValidateTopicName(topicName);

            if (adminClient is null)
            {
                throw new ArgumentNullException(nameof(adminClient));
            }

            _intTimeoutSeconds = intTimeoutSeconds;
            _adminClient = adminClient;
            _topicName = topicName;
        }

        private PartitionWatermark CreatePartitionWatermark<Key, Value>(IConsumer<Key, Value> consumer, TopicPartition topicPartition)
        {
            var watermarkOffsets = consumer.QueryWatermarkOffsets(
                                    topicPartition,
                                    TimeSpan.FromSeconds(_intTimeoutSeconds));

            return new PartitionWatermark(_topicName, watermarkOffsets, topicPartition.Partition);
        }

        public async Task<TopicWatermark> LoadWatermarksAsync<Key, Value>(Func<IConsumer<Key, Value>> consumerFactory, CancellationToken ct)
        {
            if (consumerFactory is null)
            {
                throw new ArgumentNullException(nameof(consumerFactory));
            }

            using var consumer = consumerFactory();

            try
            {
                var partitions = _adminClient.SplitTopicOnPartitions(_topicName, _intTimeoutSeconds);

                var partitionWatermarks = await Task.WhenAll(partitions.Select(
                            topicPartition => Task.Run(() =>
                            CreatePartitionWatermark(consumer, topicPartition), ct)
                                                       )).ConfigureAwait(false);

                return new TopicWatermark(partitionWatermarks.Where(item => item.IsReadyToRead()));
            }
            finally
            {
                consumer.Close();
            }
        }

        private readonly string _topicName;
        private readonly IAdminClient _adminClient;
        private readonly int _intTimeoutSeconds;
    }
}
