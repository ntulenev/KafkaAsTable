using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka;

using FluentAssertions;

using KafkaAsTable.Watermarks;

using Moq;

using Xunit;

namespace KafkaAsTable.Tests
{
    public class TopicWatermarkTests
    {
        [Fact(DisplayName = "TopicWatermark can't create with null partitions")]
        [Trait("Category", "Unit")]
        public void CantCreateTopicWatermarkWithInvalidParams()
        {

            // Arrange
            IEnumerable<PartitionWatermark> partitionWatermarks = null!;

            // Act
            var exception = Record.Exception(() => new TopicWatermark(partitionWatermarks));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermark could be created.")]
        [Trait("Category", "Unit")]
        public void CanCreateTopicWatermarkWithValidParams()
        {

            // Arrange
            var partitionWatermarks = (new Mock<IEnumerable<PartitionWatermark>>()).Object;

            // Act
            TopicWatermark result = null!;
            var exception = Record.Exception(() => result = new TopicWatermark(partitionWatermarks));

            // Assert
            exception.Should().BeNull();
            result.Watermarks.Should().BeEquivalentTo(partitionWatermarks);
        }

        [Fact(DisplayName = "Null consumer cant be assigned for TopicWatermark.")]
        [Trait("Category", "Unit")]
        public void CouldNotAssingNullConsumerForTopicWatermark()
        {

            // Arrange
            var partitionWatermarks = (new Mock<IEnumerable<PartitionWatermark>>()).Object;
            var watermark = new TopicWatermark(partitionWatermarks);
            IConsumer<object, object> consumer = null!;

            // Act
            var exception = Record.Exception(() => watermark.AssignWithConsumer(consumer));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        //[Fact(DisplayName = "Consumer could be assigned for TopicWatermark.")]
        //[Trait("Category", "Unit")]
        //public void CouldAssingConsumerForTopicWatermark()
        //{

        //    // Arrange
        //    var partitionWatermarks = (new Mock<IEnumerable<PartitionWatermark>>()).Object;
        //    var watermark = new TopicWatermark(partitionWatermarks);
        //    var consumerMock = new Mock<IConsumer<object, object>>();
        //    var consumer = consumerMock.Object;

        //    // Act
        //    var exception = Record.Exception(() => watermark.AssignWithConsumer(consumer));

        //    // Assert
        //    exception.Should().BeNull();

        //    //Add setup and mock for
        //    //Watermarks.Select
        //    //watermark.CreateTopicPartitionWithHighOffset() 
        //}
    }
}
