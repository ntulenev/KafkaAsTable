using System;
using System.Collections.Immutable;

namespace KafkaAsTable.Events
{
    public class KafkaUpdateTableArgs<Key, Value> : EventArgs where Key : notnull
    {
        public ImmutableDictionary<Key, Value> State { get; }

        public Key UpdatedKey { get; }

        public KafkaUpdateTableArgs(ImmutableDictionary<Key, Value> state, Key key)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
            UpdatedKey = key;
        }
    }
}
