using System.Collections.Immutable;

namespace KafkaAsTable.Events
{
    /// <summary>
    /// <see cref="EventArgs"/> for Init Table event.
    /// </summary>
    /// <typeparam name="Key">Message key.</typeparam>
    /// <typeparam name="Value">Message value.</typeparam>
    public class KafkaInitTableArgs<Key, Value> : EventArgs where Key : notnull
    {
        /// <summary>
        /// Table state.
        /// </summary>
        public ImmutableDictionary<Key, Value> State { get; }

        /// <summary>
        /// Creates <see cref="KafkaInitTableArgs{Key, Value}"/>.
        /// </summary>
        /// <param name="state">Table state.</param>
        public KafkaInitTableArgs(ImmutableDictionary<Key, Value> state)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
        }
    }
}
