using System;
using System.Collections.Immutable;

using FluentAssertions;

using KafkaAsTable.Events;

using Xunit;

namespace KafkaAsTable.Tests
{
    public class KafkaUpdateTableArgsTests
    {
        [Fact(DisplayName = "KafkaInitTableArgs can't be created with null state.")]
        [Trait("Category", "Unit")]
        public void CantCreateKafkaUpdateTableArgsWithNullState()
        {
            // Arrange
            ImmutableDictionary<object, object> state = null!;
            object key = new object();

            // Act
            var exception = Record.Exception(() => new KafkaUpdateTableArgs<object, object>(state, key));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "KafkaInitTableArgs can't be created with null key.")]
        [Trait("Category", "Unit")]
        public void CantCreateKafkaUpdateTableArgsWithNullKey()
        {
            // Arrange
            var state = ImmutableDictionary<object, object>.Empty;
            object key = null!;

            // Act
            var exception = Record.Exception(() => new KafkaUpdateTableArgs<object, object>(state, key));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }


        [Fact(DisplayName = "KafkaInitTableArgs can be created with valid state.")]
        [Trait("Category", "Unit")]
        public void CanCreateKafkaUpdateTableArgs()
        {
            // Arrange
            var state = ImmutableDictionary<object, object>.Empty;
            var key = new object();

            // Act
            var args = new KafkaUpdateTableArgs<object, object>(state, key);

            // Assert
            args.State.Should().BeSameAs(state);
            args.UpdatedKey.Should().BeSameAs(key);
        }
    }
}
