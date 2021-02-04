using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaAsTable.Helpers;

namespace KafkaAsTable.Model
{
    public class TopicOffsets<K, V>
    {
        public TopicOffsets(string topicName, Func<IConsumer<K, V>> consumerFactory, IAdminClient adminClient, int intTimeoutSeconds)
        {
            KafkaValidationHelper.ValidateTopicName(topicName);

            if (adminClient is null)
            {
                throw new ArgumentNullException(nameof(adminClient));
            }

            if (consumerFactory is null)
            {
                throw new ArgumentNullException(nameof(consumerFactory));
            }

            _intTimeoutSeconds = intTimeoutSeconds;

            _adminClient = adminClient;

            _topicName = topicName;

            _consumerFactory = consumerFactory;
        }

        public async Task<IEnumerable<(Partition Partition, WatermarkOffsets Offset)>> LoadWatermarksAsync(CancellationToken ct)
        {
            using var consumer = _consumerFactory();

            try
            {
                var partitions = _adminClient.SplitTopicOnPartitions(_topicName, _intTimeoutSeconds);

                var initialOffsets = await Task.WhenAll(partitions.Select(
                            topicPartition => Task.Run(() =>
                            {
                                var watermarkOffsets = consumer.QueryWatermarkOffsets(
                                    topicPartition,
                                    TimeSpan.FromSeconds(_intTimeoutSeconds));

                                return
                                (topicPartition.Partition,
                                WatermarkOffsets: watermarkOffsets);
                            }, ct))).ConfigureAwait(false);

                return initialOffsets.ToDictionary(
                    topicPartition => topicPartition.Partition,
                    topicPartition => topicPartition.WatermarkOffsets).GetAwaliableToRead();
            }
            finally
            {
                consumer.Close();
            }


        }

        private readonly string _topicName;

        private readonly IAdminClient _adminClient;

        private readonly int _intTimeoutSeconds;

        private readonly Func<IConsumer<K, V>> _consumerFactory;
    }
}
