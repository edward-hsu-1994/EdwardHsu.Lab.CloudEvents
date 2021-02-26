using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;

using Confluent.Kafka;

using Newtonsoft.Json;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EdwardHsu.Lab.CloudEvents
{
    class Program
    {
        const string TOPIC_NAME = "EdwardHsu.Lab.CloudEvents.Test";


        static void Main(string[] args)
        {
            Task.Run(() =>
            {
                KafkaConsumer();
            });
            Task.Run(() =>
            {
                KafkaProducer();
            });

            Console.Read();
        }

        static void KafkaConsumer()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "EdwardHsu.Lab.CloudEvents",
                BootstrapServers = "kafka:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<string, byte[]>(conf).Build())
            {
                var offsets = consumer.QueryWatermarkOffsets(new TopicPartition(TOPIC_NAME, new Partition(0)), TimeSpan.FromSeconds(1));

                consumer.Subscribe(TOPIC_NAME);

                while (true)
                {
                    var cr = consumer.Consume(CancellationToken.None);

                    if (cr == null) continue;
                    if (!cr.Message.IsCloudEvent())
                    {
                        throw new NotSupportedException();
                    }

                    var cloudEvent = cr.Message.ToCloudEvent();

                    Console.WriteLine($"Message: {cloudEvent.Data}");
                }
            }
        }

        static void KafkaProducer()
        {
            var conf = new ProducerConfig { BootstrapServers = "kafka:9092" };

            var sub = new System.Reactive.Subjects.Subject<int>();
            var obs = sub.Buffer(TimeSpan.FromSeconds(5));

            using (var p = new ProducerBuilder<string, byte[]>(conf).Build())
            {
                obs.Subscribe((x) =>
                {
                    p.Produce(TOPIC_NAME, CreateCloudEventMessage(x));
                });

                int i = 0;
                while (true)
                {
                    sub.OnNext(i++);
                    Console.WriteLine("Sended Message " + i);
                    Thread.Sleep(100);
                }

                // wait for up to 5 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(5));
            }

        }

        static KafkaCloudEventMessage CreateCloudEventMessage(IEnumerable<int> x)
        {
            var cloudEvent = new CloudEvent(
                   "com.github.pull.create",
                   new Uri("https://github.com/cloudevents/spec/pull/123"))
            {
                DataContentType = new ContentType("application/json"),
                Data = JsonConvert.SerializeObject(x.Select(x => new
                {
                    Msg = "Hello World! " + x,
                    Time = DateTime.Now.ToString()
                }))
            };

            return new KafkaCloudEventMessage(cloudEvent, ContentMode.Structured, new JsonEventFormatter());
        }
    }
}
