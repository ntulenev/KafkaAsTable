using System;
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;

namespace KafkaAsTable.Helpers
{
    public static class KafkaMetadataHelper
    {
        public static IEnumerable<TopicPartition> SplitTopicOnPartitions(this IAdminClient adminClient, string topicName, int timeout)
        {

            if (adminClient is null)
            {
                throw new ArgumentNullException(nameof(adminClient));
            }

            KafkaValidationHelper.ValidateTopicName(topicName);

            var topicMeta = adminClient.GetMetadata(
            topicName,
            TimeSpan.FromSeconds(timeout));

            var partitions = topicMeta.Topics.Single().Partitions;

            return partitions.Select(partition =>
                new TopicPartition(
                    topicName,
                    new Partition(partition.PartitionId)));
        }

        public static IEnumerable<(Partition Partition, WatermarkOffsets Offset)> GetAwaliableToRead(this Dictionary<Partition, WatermarkOffsets> source)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Where(kv => kv.Value.High > kv.Value.Low).Select(x => (x.Key, x.Value));
        }
    }
}
