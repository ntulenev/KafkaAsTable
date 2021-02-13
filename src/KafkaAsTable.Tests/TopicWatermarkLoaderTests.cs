using System;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using FluentAssertions;

using KafkaAsTable.Metadata;

using Moq;

using Xunit;

namespace KafkaAsTable.Tests
{
    public class TopicWatermarkLoaderTests
    {
        [Fact(DisplayName = "TopicWatermarkLoader Topic name can't be null.")]
        [Trait("Category", "Unit")]
        public void CantCreateLoaderWithEmptyTopic()
        {

            // Arrange
            TopicName topic = null!;
            var client = (new Mock<IAdminClient>()).Object;
            var timeout = 1000;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(topic, client, timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader Admin client can't be null.")]
        [Trait("Category", "Unit")]
        public void CantCreateLoaderWithEmptyAdminClient()
        {

            // Arrange
            var topic = new TopicName("test");
            IAdminClient client = null!;
            var timeout = 1000;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(topic, client, timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "TopicWatermarkLoader Timeout can't be non positive.")]
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

        [Fact(DisplayName = "TopicWatermarkLoader could be created with valid params.")]
        [Trait("Category", "Unit")]
        public void CanCreateWithValidParams()
        {
            // Arrange
            var topic = new TopicName("test");
            var client = (new Mock<IAdminClient>()).Object;
            var timeout = 1000;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(topic, client, timeout));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "Can't load watermarks with empty consumer func.")]
        [Trait("Category", "Unit")]
        public async Task CantLoadWatermarksWithNullConsumerFuncAsync()
        {
            // Arrange
            var topic = new TopicName("test");
            var client = (new Mock<IAdminClient>()).Object;
            var timeout = 1000;
            var loader = new TopicWatermarkLoader(topic, client, timeout);
            Func<IConsumer<object, object>> consumerFactory = null!;

            // Act
            var exception = await Record.ExceptionAsync(async () => _ = await loader.LoadWatermarksAsync(consumerFactory, CancellationToken.None));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        //TODO Add test on valid LoadWatermarksAsync execution
    }
}
