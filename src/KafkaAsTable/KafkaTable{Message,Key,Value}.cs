using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaAsTable.Events;
using KafkaAsTable.Helpers;

namespace KafkaAsTable
{
    /// <summary>
    /// Class that project topic in to Key-Value table
    /// </summary>
    /// <typeparam name="Message">Topic message type</typeparam>
    /// <typeparam name="Key">Table key</typeparam>
    /// <typeparam name="Value">Table value</typeparam>
    public class KafkaTable<Message, Key, Value> where Key : notnull
    {
        public event EventHandler<KafkaDumpArgs<Key, Value>>? OnDumpLoaded;

        public event EventHandler<KafkaDumpArgs<Key, Value>>? OnStateUpdated;

        public ImmutableDictionary<Key, Value> TableSnapshot { get; private set; } = null!;

        public KafkaTable(Func<Message, (Key, Value)> deserializer,
                          Func<IConsumer<Ignore, Message>> consumerFactory,
                          IAdminClient adminClient,
                          string topicName,
                          int initTimeoutSeconds = 5)
        {
            if (deserializer is null)
            {
                throw new ArgumentNullException(nameof(deserializer));
            }

            if (adminClient is null)
            {
                throw new ArgumentNullException(nameof(adminClient));
            }

            KafkaValidationHelper.ValidateTopicName(topicName);

            if (consumerFactory is null)
            {
                throw new ArgumentNullException(nameof(consumerFactory));
            }

            _adminClient = adminClient;
            _topicName = topicName;
            _deserializer = deserializer;
            _initTimeoutSecond = initTimeoutSeconds;
            _consumerFactory = consumerFactory;
        }

        private async Task<Dictionary<Partition, WatermarkOffsets>> GetOffsetsAsync(CancellationToken ct)
        {
            var metadataOfParticularTopic = _adminClient.GetMetadata(
               _topicName,
               TimeSpan.FromSeconds(_initTimeoutSecond));

            var partitionsMetadata = metadataOfParticularTopic.Topics.Single().Partitions;

            var topicPartitions = partitionsMetadata.Select(partition =>
                new TopicPartition(
                    _topicName,
                    new Partition(partition.PartitionId)));

            using var consumer = _consumerFactory();

            try
            {
                var initialOffsets = await Task.WhenAll(topicPartitions.Select(
                        topicPartition => Task.Run(() =>
                        {
                            var watermarkOffsets = consumer.QueryWatermarkOffsets(
                                topicPartition,
                                TimeSpan.FromSeconds(_initTimeoutSecond));

                            return
                            (topicPartition.Partition,
                            WatermarkOffsets: watermarkOffsets);
                        }, ct))).ConfigureAwait(false);

                return initialOffsets.ToDictionary(
                    topicPartition => topicPartition.Partition,
                    topicPartition => topicPartition.WatermarkOffsets);
            }
            finally
            {
                consumer.Close();
            }
        }

        public async Task StartUpdatingAsync(CancellationToken ct)
        {
            var offsets = await GetOffsetsAsync(ct);

            var consumedEntities = await Task.WhenAll(offsets.Where(kv => kv.Value.High > kv.Value.Low)
                .Select(kv =>
                    Task.Run(() =>
                    {
                        IConsumer<Ignore, Message>? consumer = null;

                        var entitiesInTsk = new List<KeyValuePair<Key, Value>>();

                        try
                        {
                            consumer = _consumerFactory();
                            consumer.Assign(new TopicPartition(_topicName, kv.Key));

                            ConsumeResult<Ignore, Message> result = default!;

                            do
                            {
                                result = consumer.Consume(ct);
                                var (key, value) = _deserializer(result.Message.Value);
                                entitiesInTsk.Add(new KeyValuePair<Key, Value>(key, value));


                            } while (result.Offset != kv.Value.High - 1);

                            return entitiesInTsk;
                        }
                        finally
                        {
                            consumer?.Close();
                            consumer?.Dispose();
                        }
                    }))).ConfigureAwait(false);

            var items = consumedEntities.SelectMany(singleConsumerResults => singleConsumerResults).ToList();
            TableSnapshot = ImmutableDictionary.CreateRange(items);

            OnDumpLoaded?.Invoke(this, new KafkaDumpArgs<Key, Value>(TableSnapshot));

            UpdateAfterDump(offsets, ct);
        }

        private void UpdateAfterDump(Dictionary<Partition, WatermarkOffsets> offsets, CancellationToken ct)
        {
            IConsumer<Ignore, Message>? consumer = null;

            try
            {
                consumer = _consumerFactory();

                consumer.Assign(offsets
                    .Select(kv => new TopicPartitionOffset(
                        new TopicPartition(
                            _topicName,
                            kv.Key),
                        kv.Value.High)));

                do
                {
                    var result = consumer.Consume(ct);
                    var (k, v) = _deserializer(result.Message.Value);
                    TableSnapshot = TableSnapshot.SetItem(k, v);
                    OnStateUpdated?.Invoke(this, new KafkaDumpArgs<Key, Value>(TableSnapshot));
                }
                while (!ct.IsCancellationRequested);
            }
            finally
            {
                consumer?.Close();
                consumer?.Dispose();
            }
        }

        private readonly Func<Message, (Key, Value)> _deserializer;
        private readonly IAdminClient _adminClient;
        private readonly Func<IConsumer<Ignore, Message>> _consumerFactory;
        private readonly string _topicName;
        private readonly int _initTimeoutSecond;


    }
}
