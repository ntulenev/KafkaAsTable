using System.Collections.Immutable;

using Confluent.Kafka;

using KafkaAsTable.Events;
using KafkaAsTable.Metadata;
using KafkaAsTable.Watermarks;

namespace KafkaAsTable
{
    /// <summary>
    /// Class that project topic in to Key-Value table.
    /// </summary>
    /// <typeparam name="Message">Topic message type.</typeparam>
    /// <typeparam name="Key">Table key.</typeparam>
    /// <typeparam name="Value">Table value.</typeparam>
    public class KafkaTable<Message, Key, Value> where Key : notnull
    {
        /// <summary>
        /// Event that fires when initial state is ready.
        /// </summary>
        public event EventHandler<KafkaInitTableArgs<Key, Value>>? OnDumpLoaded;

        /// <summary>
        /// Event that fires when we have new update in topic.
        /// </summary>
        public event EventHandler<KafkaUpdateTableArgs<Key, Value>>? OnStateUpdated;

        /// <summary>
        /// Current table state.
        /// </summary>
        public ImmutableDictionary<Key, Value> Snapshot { get; private set; } = null!;

        /// <summary>
        /// Creates <see cref="KafkaTable{Message, Key, Value}"/> instance.
        /// </summary>
        /// <param name="deserializer">Convertor from topic message to Key/Value data."/></param>
        /// <param name="consumerFactory">Consumer creation factory.</param>
        /// <param name="topicWatermarkLoader">Watermark loader service.</param>
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

        /// <summary>
        /// Runs reading data from topic.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        public async Task StartUpdatingAsync(CancellationToken ct)
        {
            var topicWatermark = await _topicWatermarkLoader.LoadWatermarksAsync(_consumerFactory, ct);
            var initialState = await ConsumeInitialAsync(topicWatermark, ct);
            FillSnapshot(initialState);
            ContinueFromWatermark(topicWatermark, ct);
        }

        private void FillSnapshot(IEnumerable<KeyValuePair<Key, Value>> items)
        {
            var dictionary = items.Aggregate(
                new Dictionary<Key, Value>(),
                (d, e) =>
                {
                    d[e.Key] = e.Value;
                    return d;
                });
            Snapshot = ImmutableDictionary.CreateRange(dictionary);
            OnDumpLoaded?.Invoke(this, new KafkaInitTableArgs<Key, Value>(Snapshot));
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
                    result = consumer.Consume(ct);
                    var (key, value) = _deserializer(result.Message.Value);
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

        private void ContinueFromWatermark(TopicWatermark topicWatermark, CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {
                topicWatermark.AssignWithConsumer(consumer);
                while (!ct.IsCancellationRequested)
                {
                    var result = consumer.Consume(ct);
                    var (k, v) = _deserializer(result.Message.Value);
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
