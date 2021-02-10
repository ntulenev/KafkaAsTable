using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace KafkaAsTable.Helpers
{
    /// <summary>
    /// Helper utility for Apache kafka validation.
    /// </summary>
    public static class KafkaValidationHelper
    {
        /// <summary>
        /// Validates topic name.
        /// </summary>
        /// <param name="topicName">Topic name.</param>
        public static void ValidateTopicName(string topicName)
        {
            if (topicName is null)
            {
                throw new ArgumentNullException(nameof(topicName));
            }

            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException(
                    "The topic name cannot be empty or consist of whitespaces.", nameof(topicName));
            }

            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException(
                    "The topic name cannot be empty or consist of whitespaces.", nameof(topicName));
            }

            if (topicName.Any(character => char.IsWhiteSpace(character)))
            {
                throw new ArgumentException(
                    "The topic name cannot contain whitespaces.", nameof(topicName));
            }

            if (topicName.Length > MAX_TOPIC_NAME_LENGTH)
            {
                throw new ArgumentException(
                    "The name of a topic is too long.", nameof(topicName));
            }

            if (!_topicNameCharacters.IsMatch(topicName))
            {
                throw new ArgumentException(
                    "The topic name may consist of characters 'a' to 'z', 'A' to 'Z', digits, and minus signs.", nameof(topicName));
            }
        }

        private static readonly Regex _topicNameCharacters = new Regex(
           "^[a-zA-Z0-9\\-]*$",
           RegexOptions.Compiled);

        private const int MAX_TOPIC_NAME_LENGTH = 249;
    }
}
