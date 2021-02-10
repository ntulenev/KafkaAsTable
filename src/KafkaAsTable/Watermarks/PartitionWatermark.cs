using System;

using Confluent.Kafka;

using KafkaAsTable.Helpers;

namespace KafkaAsTable.Watermarks
{

    /// <summary>
    /// Offset watermark for single partition in topic.
    /// </summary>
    public class PartitionWatermark
    {
        /// <summary>
        /// Creates partition offset watermark.
        /// </summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="offset">Raw kafka offset representation.</param>
        /// <param name="partition">Raw kafka partition representation.</param>
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

        /// <summary>
        /// Checks that partition if valid for reading.
        /// </summary>
        public bool IsReadyToRead() => _offset.High > _offset.Low;

        /// <summary>
        /// Checks that end of the partition is achieved by consumer.
        /// </summary>
        /// <typeparam name="K">Message key.</typeparam>
        /// <typeparam name="V">Message value.</typeparam>
        /// <param name="consumeResult">Consumer result.</param>
        public bool IsWatermarkAchievedBy<K, V>(ConsumeResult<K, V> consumeResult)
        {
            if (consumeResult is null)
            {
                throw new ArgumentNullException(nameof(consumeResult));
            }

            return consumeResult.Offset != _offset.High - 1;
        }

        /// <summary>
        /// Creates single-partition topic for assigning with current offset.
        /// </summary>
        public TopicPartitionOffset CreateTopicPartitionWithHighOffset() =>
            new TopicPartitionOffset(new TopicPartition(_topicName, _partition), _offset.High);

        /// <summary>
        /// Assing consumer to a partition as topic.
        /// </summary>
        /// <typeparam name="K">Message key.</typeparam>
        /// <typeparam name="V">Message value.</typeparam>
        /// <param name="consumer">Consumer.</param>
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
