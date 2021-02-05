using System;
using System.Collections.Generic;
using System.Text;

using Confluent.Kafka;

using KafkaAsTable.Helpers;

namespace KafkaAsTable.Model
{
    public class PartitionWatermark
    {
        public PartitionWatermark(string topicName, WatermarkOffsets offset, Partition partition)
        {
            KafkaValidationHelper.ValidateTopicName(topicName);

            if (offset is null)
            {
                throw new ArgumentNullException(nameof(offset));
            }

            _topicName = topicName;

            _offset = offset;

            _partition = partition;
        }

        public bool IsReadyToRead() => _offset.High > _offset.Low;

        public TopicPartitionOffset CreateTopicPartitionWithHighOffset() => new TopicPartitionOffset(new TopicPartition(_topicName, _partition), _offset.High);

        public TopicPartition CreatePartition() => new TopicPartition(_topicName, _partition);

        public Partition Partition => _partition;

        public WatermarkOffsets Watermark => _offset;

        private readonly Partition _partition;

        private readonly WatermarkOffsets _offset;

        private readonly string _topicName;

    }
}
