using System;

using Confluent.Kafka;

using KafkaAsTable.Helpers;

namespace KafkaAsTable.Model
{
    public class PartitionWatermark
    {
        public PartitionWatermark(string topicName,
                                  WatermarkOffsets offset,
                                  Partition partition)
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

        public bool IsWatermarkAchievedBy<K, V>(ConsumeResult<K, V> consumeResult)
        {
            if (consumeResult is null)
            {
                throw new ArgumentNullException(nameof(consumeResult));
            }

            return consumeResult.Offset != _offset.High - 1;
        }

        public TopicPartitionOffset CreateTopicPartitionWithHighOffset() =>
            new TopicPartitionOffset(new TopicPartition(_topicName, _partition), _offset.High);

        public void AssingWithConsumer<K,V>(IConsumer<K, V> consumer)
        {
            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            consumer.Assign(new TopicPartition(_topicName, _partition));
        }

        private readonly Partition _partition;
        private readonly WatermarkOffsets _offset;
        private readonly string _topicName;

    }
}
