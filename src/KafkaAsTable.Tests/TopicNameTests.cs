using System;

using FluentAssertions;

using KafkaAsTable.Metadata;

using Xunit;

namespace KafkaAsTable.Tests
{
    public class TopicNameTests
    {
        [Fact(DisplayName = "Topic name can't be null.")]
        [Trait("Category", "Unit")]
        public void CantCreateNullTopicName()
        {

            // Arrange
            string name = null!;

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "Topic name can't be empty.")]
        [Trait("Category", "Unit")]
        public void CantCreateEmptyTopicName()
        {

            // Arrange
            var name = string.Empty;

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't be whitespaces.")]
        [Trait("Category", "Unit")]
        public void CantCreateWhitespacesTopicName()
        {

            // Arrange
            var name = "     ";

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't have any whitespaces.")]
        [Trait("Category", "Unit")]
        public void CantCreateAnyWhitespacesTopicName()
        {

            // Arrange
            var name = "topic name";

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't have long names.")]
        [Trait("Category", "Unit")]
        public void CantCreateLongTopicName()
        {

            // Arrange
            var name = new string('x', 250);

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't have bad symbols names.")]
        [Trait("Category", "Unit")]
        public void CantCreateBadTopicName()
        {

            // Arrange
            var name = "ы?:%";

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }
    }
}
