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
        public event EventHandler<KafkaTableArgs<Key, Value>>? OnDumpLoaded;

        public event EventHandler<KafkaTableArgs<Key, Value>>? OnStateUpdated;

        public ImmutableDictionary<Key, Value> Snapshot { get; private set; } = null!;

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
            _initTimeoutSeconds = initTimeoutSeconds;
            _consumerFactory = consumerFactory;
        }

        private async Task<Dictionary<Partition, WatermarkOffsets>> GetOffsetsAsync(CancellationToken ct)
        {
            var topicPartitions = _adminClient.SplitTopicOnPartitions(_topicName, _initTimeoutSeconds);

            using var consumer = _consumerFactory();

            try
            {
                var initialOffsets = await Task.WhenAll(topicPartitions.Select(
                        topicPartition => Task.Run(() =>
                        {
                            var watermarkOffsets = consumer.QueryWatermarkOffsets(
                                topicPartition,
                                TimeSpan.FromSeconds(_initTimeoutSeconds));

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
            var offsets = await GetOffsetsAsync(ct).ConfigureAwait(false);

            var consumedEntities = await Task.WhenAll(offsets.GetAwaliableToRead()
                .Select(endOfPartition =>
                    Task.Run(() =>
                    {
                        using var consumer = _consumerFactory();

                        var items = new List<KeyValuePair<Key, Value>>();

                        try
                        {
                            consumer.Assign(new TopicPartition(_topicName, endOfPartition.Partition));

                            ConsumeResult<Ignore, Message> result = default!;
                            do
                            {
                                var (key, value) = ConsumeItem(consumer, ct);
                                items.Add(new KeyValuePair<Key, Value>(key, value));

                            } while (result.IsWatermarkAchieved(endOfPartition.Offset));

                            return items;
                        }
                        finally
                        {
                            consumer?.Close();
                        }

                    }))).ConfigureAwait(false);

            var items = consumedEntities.SelectMany(singleConsumerResults => singleConsumerResults);

            Snapshot = ImmutableDictionary.CreateRange(items);

            OnDumpLoaded?.Invoke(this, new KafkaTableArgs<Key, Value>(Snapshot));

            ContinueUpdateAfterDump(offsets, ct);
        }

        private (Key key, Value value) ConsumeItem(IConsumer<Ignore, Message> consumer, CancellationToken ct)
        {
            var result = consumer.Consume(ct);
            return _deserializer(result.Message.Value);
        }

        private void ContinueUpdateAfterDump(Dictionary<Partition, WatermarkOffsets> offsets, CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {

                consumer.Assign(offsets
                    .Select(oldEndOfPartition => new TopicPartitionOffset(
                        new TopicPartition(
                            _topicName,
                            oldEndOfPartition.Key),
                        oldEndOfPartition.Value.High)));

                do
                {
                    var (k, v) = ConsumeItem(consumer, ct);
                    Snapshot = Snapshot.SetItem(k, v);

                    OnStateUpdated?.Invoke(this, new KafkaTableArgs<Key, Value>(Snapshot));
                }
                while (!ct.IsCancellationRequested);
            }
            finally
            {
                consumer?.Close();
            }
        }

        private readonly Func<Message, (Key, Value)> _deserializer;
        private readonly IAdminClient _adminClient;
        private readonly Func<IConsumer<Ignore, Message>> _consumerFactory;
        private readonly string _topicName;
        private readonly int _initTimeoutSeconds;
    }
}
