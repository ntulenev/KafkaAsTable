using System;
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;

namespace KafkaAsTable.Helpers
{
    public static class KafkaHelpers
    {
        public static IEnumerable<TopicPartition> SplitTopicOnPartitions(this IAdminClient adminClient, string topicName, int timeout)
        {
            if (adminClient is null)
            {
                throw new ArgumentNullException(nameof(adminClient));
            }

            KafkaValidationHelper.ValidateTopicName(topicName);

            var topicMeta = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(timeout));

            var partitions = topicMeta.Topics.Single().Partitions;

            return partitions.Select(partition => new TopicPartition(topicName, new Partition(partition.PartitionId)));
        }

        public static IEnumerable<(Partition Partition, WatermarkOffsets Offset)> GetAwaliableToRead(this Dictionary<Partition, WatermarkOffsets> source)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Where(kv => kv.Value.High > kv.Value.Low).Select(x => (x.Key, x.Value));
        }

        public static bool IsWatermarkAchieved<K, V>(this ConsumeResult<K, V> consumeResult, WatermarkOffsets watermark)
        {
            if (consumeResult is null)
            {
                throw new ArgumentNullException(nameof(consumeResult));
            }

            if (watermark is null)
            {
                throw new ArgumentNullException(nameof(watermark));
            }

            return consumeResult.Offset != watermark.High - 1;
        }

        public static void AssignToOffset<K, V>(this IConsumer<K, V> consumer, Dictionary<Partition, WatermarkOffsets> offsets, string topicName)
        {
            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            if (offsets is null)
            {
                throw new ArgumentNullException(nameof(offsets));
            }

            KafkaValidationHelper.ValidateTopicName(topicName);

            consumer.Assign(offsets.Select(oldEndOfPartition => new TopicPartitionOffset(new TopicPartition(topicName, oldEndOfPartition.Key), oldEndOfPartition.Value.High)));
        }
    }
}
