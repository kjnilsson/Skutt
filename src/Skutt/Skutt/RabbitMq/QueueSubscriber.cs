﻿using System.IO;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Skutt.Contract;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Skutt.RabbitMq
{
    internal class QueueSubscriber : IDisposable
    {
        private readonly string queue;
        private readonly MessageTypeRegistry registry;
        private readonly Action<object> queueAdd;
        private QueueingBasicConsumer consumer;
        private Task task;
        private CancellationTokenSource cts;

        public QueueSubscriber(string queue,
                               MessageTypeRegistry registry,
                               Action<object> messageHandler)
        {
            Preconditions.Require(queue, "queue");
            Preconditions.Require(registry, "registry");
            Preconditions.Require(messageHandler, "messageHandler");

            this.queue = queue;
            this.registry = registry;
            this.queueAdd = messageHandler;
        }

        public void StartConsuming(IConnection connection)
        {
            if (task != null)
            {
                Console.WriteLine("adding continuation sub");
         
                this.cts = new CancellationTokenSource();
                this.task.ContinueWith(t =>
                                        {
                                            try
                                            {
                                                ProcessQueue(connection);
                                            }
                                            catch (IOException e)
                                            {
                                                Console.WriteLine("connection interrupted " + e.Message);
                                            }
                                        },
                                       cts.Token,
                                       TaskContinuationOptions.LongRunning,
                                       TaskScheduler.Default);
                return;
            }

            this.cts = new CancellationTokenSource();

            this.task = Task.Factory.StartNew(() =>
                {
                    try
                    {
                        ProcessQueue(connection);
                    }
                    catch (IOException e)
                    {
                        Console.WriteLine("connection interrupted " + e.Message);
                    }

                },
                cts.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        private void ProcessQueue(IConnection connection)
        {
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue, true, false, false, null);
                consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume(queue, false, consumer);

                while (true)
                {
                    var deliveryEventArgs = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
                    
                    if (deliveryEventArgs == null) // poison
                    {
                        Console.WriteLine("Poison message received.");
                        break;
                    }

                    var body = deliveryEventArgs.Body;
                    var typeLen = BitConverter.ToInt16(body, 0);
                    var messageType = registry.GetType(deliveryEventArgs.BasicProperties.Type);

                    if (messageType != null)
                    {
                        var serializedMessage = Encoding.UTF8.GetString(body, typeLen + 2,
                                                                        body.Length - typeLen - 2);
                        var messageObject = JsonConvert.DeserializeObject(serializedMessage, messageType);

                        queueAdd(messageObject);
                        channel.BasicAck(deliveryEventArgs.DeliveryTag, false);
                    }
                    else
                    {
                        //TODO messageHandler dead letter queue
                        channel.BasicReject(deliveryEventArgs.DeliveryTag, false);
                    }
                }
            }
        }

        public void Stop()
        {
            if (cts != null)
            {
                cts.Cancel();
            }
        }

        public void Dispose()
        {
            if(cts != null) cts.Dispose();
        }
    }
}
