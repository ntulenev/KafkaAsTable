using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaAsTable;
using KafkaAsTable.Metadata;

using Newtonsoft.Json;

namespace ExampleApp
{
    public record KafkaMessage(int Key, Guid Value);

    /// <summary>
    /// This example shows how to read table from topic
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Task that write data in kafka every 1 second to emulate updates
        /// </summary>
        private static async Task FillKafkaWithDataAsync(string topicName, CancellationToken ct)
        {
            var config = new ProducerConfig { BootstrapServers = GetServers() };

            using var p = new ProducerBuilder<Null, string>(config).Build();
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var newMessage = new KafkaMessage(_rnd.Next(1, 5), Guid.NewGuid());
                    var stringMessage = JsonConvert.SerializeObject(newMessage);
                    var dr = await p.ProduceAsync(topicName, new Message<Null, string> { Value = stringMessage }, ct);
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
                await Task.Delay(1000, ct);
            }
        }

#pragma warning disable IDE1006 // Naming Styles
#pragma warning disable IDE0060 // Remove unused parameter
        public static async Task Main(string[] args)
#pragma warning restore IDE0060 // Remove unused parameter
#pragma warning restore IDE1006 // Naming Styles
        {
            var cts = new CancellationTokenSource();

            var topicName = "table-test-topic";

            Console.WriteLine("Start updating topic");
            var kafkaLoaderTask = FillKafkaWithDataAsync(topicName, cts.Token);
            Console.WriteLine("Wait 5 seconds to fill topic before init table");
            await Task.Delay(5_000);

            var kTableTask = RunKTableAsync(topicName, cts.Token);

            await Task.WhenAll(kTableTask, kafkaLoaderTask);
        }

        /// <summary>
        /// This task reads data from topic to in-memory table
        /// </summary>
        private static async Task RunKTableAsync(string topicName, CancellationToken ct)
        {
            static (int, Guid) deserializer(string message)
            {
                var msg = JsonConvert.DeserializeObject<KafkaMessage>(message);
                return (msg.Key, msg.Value);
            }

            static IConsumer<Ignore, string> createConsumer()
            {
                var conf = new ConsumerConfig
                {
                    BootstrapServers = GetServers(),
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    GroupId = Guid.NewGuid().ToString()
                };

                return new ConsumerBuilder<Ignore, string>(conf).Build();
            }

            var adminConfig = new AdminClientConfig()
            {
                BootstrapServers = GetServers()
            };

            var adminClient = new AdminClientBuilder(adminConfig).Build();

            var waterMarkLoader = new TopicWatermarkLoader(new TopicName(topicName), adminClient, 1000);

            var kTable = new KafkaTable<string, int, Guid>(deserializer, createConsumer, waterMarkLoader);
            kTable.OnDumpLoaded += KTableOnDumpLoaded;
            kTable.OnStateUpdated += KTableOnStateUpdated;
            await kTable.StartUpdatingAsync(ct);
        }

        private static void KTableOnStateUpdated(object? sender, KafkaAsTable.Events.KafkaUpdateTableArgs<int, Guid> e)
            => Console.WriteLine($"Update key {e.UpdatedKey} with value {e.State[e.UpdatedKey]}");

        private static void KTableOnDumpLoaded(object? sender, KafkaAsTable.Events.KafkaInitTableArgs<int, Guid> e)
        {
            Console.WriteLine($"Initial state loaded");
            foreach (var item in e.State)
            {
                Console.WriteLine($"\t {item.Key}-{item.Value}");
            }
        }

        private static string GetServers() => string.Join(",", _bootstrapServers);

        private static readonly Random _rnd = new Random();

        private static readonly List<string> _bootstrapServers = new List<string>()
        {
            //TODO Add test cluster servers 
        };
    }
}
