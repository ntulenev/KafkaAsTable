using System;
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;

namespace KafkaAsTable.Watermarks
{
    public class TopicWatermark
    {
        public TopicWatermark(IEnumerable<PartitionWatermark> partitionWatermarks)
        {
            if (partitionWatermarks is null)
            {
                throw new ArgumentNullException(nameof(partitionWatermarks));
            }
            Watermarks = partitionWatermarks;
        }

        public IEnumerable<PartitionWatermark> Watermarks { get; }

        public void AssignWithConsumer<K, V>(IConsumer<K, V> consumer)
        {
            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            consumer.Assign(Watermarks.Select(watermark => watermark.CreateTopicPartitionWithHighOffset()));
        }
    }
}
