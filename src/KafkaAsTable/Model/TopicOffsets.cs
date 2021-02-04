using System;
using System.Collections.Generic;
using System.Text;

using KafkaAsTable.Helpers;

namespace KafkaAsTable.Model
{
    public class TopicOffsets
    {
        private readonly string _topicName;

        public TopicOffsets(string topicName)
        {
            KafkaValidationHelper.ValidateTopicName(topicName);

            _topicName = topicName;
        }

        // TODO Add topic name, offset and consumer integration logic

        // All offsets complicated logic should be moved in this class
    }
}
