using System.Collections.Immutable;

using FluentAssertions;

using KafkaAsTable.Events;

using Xunit;

namespace KafkaAsTable.Tests
{
    public class KafkaInitTableArgsTests
    {
        [Fact(DisplayName = "KafkaInitTableArgs can't be created with null state.")]
        [Trait("Category", "Unit")]
        public void CantCreateKafkaInitTableArgsWithNullState()
        {
            // Arrange
            ImmutableDictionary<object, object> state = null!;

            // Act
            var exception = Record.Exception(() => new KafkaInitTableArgs<object, object>(state));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "KafkaInitTableArgs can be created with valid state.")]
        [Trait("Category", "Unit")]
        public void CanCreateKafkaInitTableArgs()
        {
            // Arrange
            var state = ImmutableDictionary<object, object>.Empty;

            // Act
            var args = new KafkaInitTableArgs<object, object>(state);

            // Assert
            args.State.Should().BeSameAs(state);
        }
    }
}
