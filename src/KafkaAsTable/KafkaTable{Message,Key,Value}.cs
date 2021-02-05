using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaAsTable.Events;
using KafkaAsTable.Metadata;
using KafkaAsTable.Model;

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
        public event EventHandler<KafkaInitTableArgs<Key, Value>>? OnDumpLoaded;

        public event EventHandler<KafkaUpdateTableArgs<Key, Value>>? OnStateUpdated;

        public ImmutableDictionary<Key, Value> Snapshot { get; private set; } = null!;

        public KafkaTable(Func<Message, (Key, Value)> deserializer,
                          Func<IConsumer<Ignore, Message>> consumerFactory,
                          ITopicWatermarkLoader topicWatermarkLoader)
        {
            if (deserializer is null)
            {
                throw new ArgumentNullException(nameof(deserializer));
            }

            if (topicWatermarkLoader is null)
            {
                throw new ArgumentNullException(nameof(topicWatermarkLoader));
            }

            if (consumerFactory is null)
            {
                throw new ArgumentNullException(nameof(consumerFactory));
            }

            _deserializer = deserializer;
            _consumerFactory = consumerFactory;
            _topicWatermarkLoader = topicWatermarkLoader;
        }

        private IEnumerable<KeyValuePair<Key, Value>> ConsumeFromWatermark(PartitionWatermark watermark, CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {
                watermark.AssingWithConsumer(consumer);
                ConsumeResult<Ignore, Message> result = default!;
                do
                {
                    var (key, value) = ConsumeItem(consumer, ct);
                    yield return new KeyValuePair<Key, Value>(key, value);

                } while (watermark.IsWatermarkAchievedBy(result));
            }
            finally
            {
                consumer?.Close();
            }
        }

        private async Task<IEnumerable<KeyValuePair<Key, Value>>> ConsumeInitialAsync
            (TopicWatermark topicWatermark,
            CancellationToken ct)
        {
            var consumedEntities = await Task.WhenAll(topicWatermark.Watermarks
                .Select(watermark =>
                            Task.Run(() => ConsumeFromWatermark(watermark, ct))
                       )
                ).ConfigureAwait(false);

            return consumedEntities.SelectMany(сonsumerResults => сonsumerResults);
        }

        public async Task StartUpdatingAsync(CancellationToken ct)
        {
            var topicWatermark = await _topicWatermarkLoader.LoadWatermarksAsync(_consumerFactory, ct);
            var initialState = await ConsumeInitialAsync(topicWatermark, ct);
            Snapshot = ImmutableDictionary.CreateRange(initialState);
            OnDumpLoaded?.Invoke(this, new KafkaInitTableArgs<Key, Value>(Snapshot));
            ContinueFromWatermark(topicWatermark, ct);
        }

        private (Key key, Value value) ConsumeItem(IConsumer<Ignore, Message> consumer, CancellationToken ct)
        {
            var result = consumer.Consume(ct);
            return _deserializer(result.Message.Value);
        }

        private void ContinueFromWatermark(TopicWatermark topicWatermark, CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {
                topicWatermark.AssignWithConsumer(consumer);
                while (!ct.IsCancellationRequested)
                {
                    var (k, v) = ConsumeItem(consumer, ct);
                    Snapshot = Snapshot.SetItem(k, v);
                    OnStateUpdated?.Invoke(this, new KafkaUpdateTableArgs<Key, Value>(Snapshot, k));
                }
            }
            finally
            {
                consumer?.Close();
            }
        }

        private readonly Func<Message, (Key, Value)> _deserializer;
        private readonly Func<IConsumer<Ignore, Message>> _consumerFactory;
        private readonly ITopicWatermarkLoader _topicWatermarkLoader;
    }
}
