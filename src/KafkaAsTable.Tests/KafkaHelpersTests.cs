using System;
using System.Linq;
using System.Collections.Generic;

using Confluent.Kafka;

using Xunit;

using KafkaAsTable.Helpers;
using KafkaAsTable.Metadata;

using FluentAssertions;

using Moq;


namespace KafkaAsTable.Tests
{
    public class KafkaHelpersTests
    {
        [Fact(DisplayName = "SplitTopicOnPartitions cant be run on null admin client")]
        [Trait("Category", "Unit")]
        public void CantCreateSplitTopicWithNullAdminClient()
        {

            // Arrange
            IAdminClient client = null!;
            var topic = new TopicName("Test");
            var timeout = 100;

            // Act
            var exception = Record.Exception(() => client.SplitTopicOnPartitions(topic, timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SplitTopicOnPartitions cant be run on null topic name")]
        [Trait("Category", "Unit")]
        public void CantCreateSplitTopicWithNullNameClient()
        {

            // Arrange
            var clientMoq = new Mock<IAdminClient>();
            TopicName topic = null!;
            var timeout = 100;

            // Act
            var exception = Record.Exception(() => clientMoq.Object.SplitTopicOnPartitions(topic, timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "SplitTopicOnPartitions cant be run on non positive timeout")]
        [InlineData(0)]
        [InlineData(-1)]
        [Trait("Category", "Unit")]
        public void CantCreateSplitTopicWithNonPositiveTimeout(int timeout)
        {

            // Arrange
            var clientMoq = new Mock<IAdminClient>();
            var topic = new TopicName("Test");

            // Act
            var exception = Record.Exception(() => clientMoq.Object.SplitTopicOnPartitions(topic, timeout));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "SplitTopicCouldBeRun with valid params")]
        [Trait("Category", "Unit")]
        public void SplitTopicCouldBeRunOnValidParams()
        {

            // Arrange
            var clientMoq = new Mock<IAdminClient>();
            var topic = new TopicName("Test");
            var timeout = 100;

            var borkerMeta = new BrokerMetadata(1, "testHost", 1000);

            var partitionMeta = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);

            var topicMeta = new TopicMetadata(topic.Value, new[] { partitionMeta }.ToList(), null);

            var meta = new Confluent.Kafka.Metadata(
                    new[] { borkerMeta }.ToList(),
                    new[] { topicMeta }.ToList(), 1, "test"
                    );

            clientMoq.Setup(c => c.GetMetadata(topic.Value, TimeSpan.FromSeconds(timeout))).Returns(meta);

            IEnumerable<TopicPartition> result = null!;

            // Act
            var exception = Record.Exception(() =>
                {
                    result = clientMoq.Object.SplitTopicOnPartitions(topic, timeout).ToList();
                });

            // Assert
            exception.Should().BeNull();
            result.Should().NotBeEmpty();
            result.Should().ContainSingle();
            result.Single().Topic.Should().Be(topic.Value);
            result.Single().Partition.Value.Should().Be(partitionMeta.PartitionId);
        }
    }
}
