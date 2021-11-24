using Confluent.Kafka;

using FluentAssertions;

using KafkaAsTable.Metadata;
using KafkaAsTable.Watermarks;

using Moq;

using Xunit;

namespace KafkaAsTable.Tests
{
    public class PartitionWatermarkTests
    {
        [Fact(DisplayName = "PartitionWatermark can't create with null name.")]
        [Trait("Category", "Unit")]
        public void CantCreatePartitionWatermarkWithNullName()
        {

            // Arrange
            TopicName topicName = null!;
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            // Act
            var exception = Record.Exception(() => new PartitionWatermark(topicName, offsets, partition));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "PartitionWatermark can't create with null offets.")]
        [Trait("Category", "Unit")]
        public void CantCreatePartitionWatermarkWithNullWatermarkOffsets()
        {

            // Arrange
            var topicName = new TopicName("Test");
            WatermarkOffsets offsets = null!;
            var partition = new Partition(1);

            // Act
            var exception = Record.Exception(() => new PartitionWatermark(topicName, offsets, partition));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "PartitionWatermark can be created with valid params.")]
        [Trait("Category", "Unit")]
        public void PartitionWatermarkCanBeCreated()
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            PartitionWatermark result = null!;

            // Act
            var exception = Record.Exception(() => result = new PartitionWatermark(topicName, offsets, partition));

            // Assert
            exception.Should().BeNull();

            result.Partition.Should().Be(partition);
            result.Offset.Should().Be(offsets);
            result.TopicName.Should().Be(topicName);
        }

        [Theory(DisplayName = "PartitionWatermark is ready to read only valid offsets.")]
        [InlineData(0, 1, true)]
        [InlineData(1, 0, false)]
        [Trait("Category", "Unit")]
        public void IsReadyToReadReactsCorrectlyOnOffsetValues(int startOffset, int endOffset, bool condition)
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(startOffset), new Offset(endOffset));
            var partition = new Partition(1);

            var pw = new PartitionWatermark(topicName, offsets, partition);
            var result = false;

            // Act
            var exception = Record.Exception(() => result = pw.IsReadyToRead());

            // Assert
            exception.Should().BeNull();

            result.Should().Be(condition);
        }

        [Fact(DisplayName = "IsWatermarkAchievedBy faild on null result.")]
        [Trait("Category", "Unit")]
        public void IsWatermarkAchievedByFailsOnNullResult()
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            ConsumeResult<object, object> result = null!;

            var pw = new PartitionWatermark(topicName, offsets, partition);

            // Act
            var exception = Record.Exception(() => pw.IsWatermarkAchievedBy(result));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "IsWatermarkAchievedBy reacts on end offset.")]
        [Trait("Category", "Unit")]
        public void IsWatermarkAchievedByReatcsOnRightOffset()
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            var result = new ConsumeResult<object, object>()
            {
                Offset = offsets.High
            };

            var pw = new PartitionWatermark(topicName, offsets, partition);
            var status = false;

            // Act
            var exception = Record.Exception(() => status = pw.IsWatermarkAchievedBy(result));

            // Assert
            exception.Should().BeNull();
            status.Should().BeTrue();
        }

        [Fact(DisplayName = "IsWatermarkAchievedBy reacts on non end offset.")]
        [Trait("Category", "Unit")]
        public void IsWatermarkAchievedByReatcsOnNotRightOffset()
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            var result = new ConsumeResult<object, object>()
            {
                Offset = offsets.Low
            };

            var pw = new PartitionWatermark(topicName, offsets, partition);
            var status = false;

            // Act
            var exception = Record.Exception(() => status = pw.IsWatermarkAchievedBy(result));

            // Assert
            exception.Should().BeNull();
            status.Should().BeFalse();
        }

        [Fact(DisplayName = "TopicPartitionOffset could be created from PartitionWatermark.")]
        [Trait("Category", "Unit")]
        public void TopicPartitionOffsetCreatesCorrectly()
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);
            var pw = new PartitionWatermark(topicName, offsets, partition);

            TopicPartitionOffset res = null!;

            // Act
            var exception = Record.Exception(() => res = pw.CreateTopicPartitionWithHighOffset());

            // Assert
            exception.Should().BeNull();
            res.Offset.Should().Be(offsets.High);
            res.Topic.Should().Be(topicName.Value);
            res.TopicPartition.Partition.Should().Be(partition);
            res.TopicPartition.Topic.Should().Be(topicName.Value);
        }

        [Fact(DisplayName = "AssingWithConsumer fails on null consumer.")]
        [Trait("Category", "Unit")]
        public void AssingWithConsumerFailsOnNullConsumer()
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);
            var pw = new PartitionWatermark(topicName, offsets, partition);
            IConsumer<object, object> consumer = null!;

            // Act
            var exception = Record.Exception(() => pw.AssingWithConsumer(consumer));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "PartitionWatermark could be assing to consumer.")]
        [Trait("Category", "Unit")]
        public void PartitionWatermarkCouldBeAssingToConsumer()
        {

            // Arrange
            var topicName = new TopicName("Test");
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);
            var pw = new PartitionWatermark(topicName, offsets, partition);

            var consumerMock = new Mock<IConsumer<object, object>>();
            var consumer = consumerMock.Object;

            // Act
            var exception = Record.Exception(() => pw.AssingWithConsumer(consumer));

            // Assert
            exception.Should().BeNull();
            consumerMock.Verify(x => x.Assign(It.Is<TopicPartition>(a => a.Topic == topicName.Value && a.Partition == partition)), Times.Once);
        }
    }
}
