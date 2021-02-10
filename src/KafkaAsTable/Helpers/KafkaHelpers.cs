using System;
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;

namespace KafkaAsTable.Helpers
{
    /// <summary>
    /// Helper utility for Apache kafka operations.
    /// </summary>
    public static class KafkaHelpers
    {
        /// <summary>
        /// Splits apache kafka topic on partitions.
        /// </summary>
        /// <param name="adminClient">Kafka admin client.</param>
        /// <param name="topicName">Topic name.</param>
        /// <param name="timeout">Timeout in seconds for loading watermarks.</param>
        public static IEnumerable<TopicPartition> SplitTopicOnPartitions(this IAdminClient adminClient,
                                                                              string topicName,
                                                                              int timeout)
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
    }
}
