using System;

using Confluent.Kafka;

using FluentAssertions;

using KafkaAsTable.Metadata;

using Moq;

using Xunit;

namespace KafkaAsTable.Tests
{
    public class TopicWatermarkLoaderTests
    {
        [Fact(DisplayName = "Topic name can't be null.")]
        [Trait("Category", "Unit")]
        public void CantCreateLoaderWithEmptyTopic()
        {

            // Arrange
            TopicName topic = null!;
            IAdminClient client = (new Mock<IAdminClient>()).Object;
            int timeout = 1000;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(topic,client,timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "Admin client can't be null.")]
        [Trait("Category", "Unit")]
        public void CantCreateLoaderWithEmptyAdminClient()
        {

            // Arrange
            var topic = new TopicName("test");
            IAdminClient client = null!;
            int timeout = 1000;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(topic, client, timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "Timeout can't be non positive.")]
        [Trait("Category", "Unit")]
        [InlineData(0)]
        [InlineData(-1)]
        public void CantCreateLoaderInvalidTimeout(int timeout)
        {
            // Arrange
            var topic = new TopicName("test");
            var client = (new Mock<IAdminClient>()).Object;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(topic, client, timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }
    }
}
