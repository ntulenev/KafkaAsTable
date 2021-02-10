using System;
using System.Collections.Immutable;

namespace KafkaAsTable.Events
{
    /// <summary>
    /// <see cref="EventArgs"/> for update table event.
    /// </summary>
    /// <typeparam name="Key">Message key.</typeparam>
    /// <typeparam name="Value">Message value.</typeparam>
    public class KafkaUpdateTableArgs<Key, Value> : EventArgs where Key : notnull
    {
        /// <summary>
        /// Table state.
        /// </summary>
        public ImmutableDictionary<Key, Value> State { get; }

        /// <summary>
        /// Updated key.
        /// </summary>
        public Key UpdatedKey { get; }

        /// <summary>
        /// Creates <see cref="KafkaUpdateTableArgs{Key, Value}"/>.
        /// </summary>
        /// <param name="state">Table state.</param>
        /// <param name="key">Updated key.</param>
        public KafkaUpdateTableArgs(ImmutableDictionary<Key, Value> state, Key key)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
            UpdatedKey = key;
        }
    }
}
