using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using FluentAssertions;

using KafkaAsTable.Metadata;
using KafkaAsTable.Watermarks;

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

        [Fact(DisplayName = "Can load watermarks with valid params.")]
        [Trait("Category", "Unit")]
        public async Task CanLoadWatermarksWithValidParamsAsync()
        {
            // Arrange
            var topic = new TopicName("test");
            var clientMock = new Mock<IAdminClient>();
            var client = clientMock.Object;
            var timeout = 1000;
            var loader = new TopicWatermarkLoader(topic, client, timeout);
            var consumerMock = new Mock<IConsumer<object, object>>();
            IConsumer<object, object> consumerFactory() => consumerMock.Object;

            var adminClientPartition = new TopicPartition(topic.Value, new Partition(1));

            var adminParitions = new[] { adminClientPartition };

            var borkerMeta = new BrokerMetadata(1, "testHost", 1000);

            var partitionMeta = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);

            var topicMeta = new TopicMetadata(topic.Value, new[] { partitionMeta }.ToList(), null);

            var meta = new Confluent.Kafka.Metadata(
                    new[] { borkerMeta }.ToList(),
                    new[] { topicMeta }.ToList(), 1, "test"
                    );

            clientMock.Setup(c => c.GetMetadata(topic.Value, TimeSpan.FromSeconds(timeout))).Returns(meta);

            var offets = new WatermarkOffsets(new Offset(1), new Offset(2));

            consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition, TimeSpan.FromSeconds(timeout))).Returns(offets);

            TopicWatermark result = null!;

            // Act
            var exception = await Record.ExceptionAsync(async () => result = await loader.LoadWatermarksAsync(consumerFactory, CancellationToken.None));

            // Assert
            exception.Should().BeNull();
            consumerMock.Verify(x => x.Close(), Times.Once);
            consumerMock.Verify(x => x.Dispose(), Times.Once);
            result.Should().NotBeNull();
            var watermarks = result.Watermarks.ToList();
            watermarks.Should().ContainSingle();
            clientMock.Verify(c => c.GetMetadata(topic.Value, TimeSpan.FromSeconds(timeout)), Times.Once);
            consumerMock.Verify(x => x.QueryWatermarkOffsets(adminClientPartition, TimeSpan.FromSeconds(timeout)), Times.Once);
        }
    }
}
