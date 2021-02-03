using System;
using System.Collections.Immutable;

namespace KafkaAsTable.Events
{
    public class KafkaInitTableArgs<Key, Value> : EventArgs where Key : notnull
    {
        public ImmutableDictionary<Key, Value> State { get; }

        public KafkaInitTableArgs(ImmutableDictionary<Key, Value> state)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
        }
    }
}
