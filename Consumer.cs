using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using Confluent.Kafka;
using KafkaProducer;

namespace KafkaConsumer
{
    internal class Consumer
    {
        private readonly ConsumerConfig _config;
        private readonly CancellationTokenSource cts;
        private Action<ConsumeResult<Ignore, string>> messageRecievedHandler = MessageRecievedHandler;

        private static void MessageRecievedHandler(ConsumeResult<Ignore, string> consumeResult)
        {
            var timeStamp = JsonSerializer.Deserialize<TimeStampRecord>(consumeResult.Message.Value);

            Console.WriteLine($"HeartBeat Event with Id: {timeStamp.EventId}, ParrentEvent: {timeStamp.ParentEvent}, TimeStamp: {timeStamp.TimeStamp}, with Hash:{BitConverter.ToString(timeStamp.HashValue)}, on Partition: {consumeResult.Partition}");
        }

        public Consumer()
        {
            this._config = new ConsumerConfig
            {
                BootstrapServers = "localhost:29092,localhost39092",
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            Run();
        }

        private void Run()
        {
            bool cancelled = false;

            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build()){
                consumer.Subscribe("HeartBeats2");
                do
                {
                    var message = consumer.Consume(cts.Token);
                    messageRecievedHandler(message);

                } while (!cts.IsCancellationRequested);
                consumer.Close();
            }
        }



    }
}
