using System;
using System.Collections.Immutable;

namespace KafkaAsTable.Events
{
    public class KafkaDumpArgs<Key, Value> : EventArgs where Key : notnull
    {
        public ImmutableDictionary<Key, Value> State { get; private set; }

        public KafkaDumpArgs(ImmutableDictionary<Key, Value> state)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
        }
    }
}
