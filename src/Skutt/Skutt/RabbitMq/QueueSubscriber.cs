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
            Action<object> messageHandler,
            bool startImmediately = false)
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
            if (task != null && (task.IsCanceled || task.IsCompleted || task.IsFaulted) == false)
            { 
                // assume a task is running and hasnt failed
                return;
            }

            this.cts = new CancellationTokenSource();

            this.task = Task.Factory.StartNew(() =>
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ConfirmSelect();
                    channel.QueueDeclare(queue, true, false, false, null);
                    consumer = new QueueingBasicConsumer(channel);
                    channel.BasicConsume(queue, false, consumer);


                    while (true)
                    {
                        var deliveryEventArgs = consumer.Queue.Dequeue() as BasicDeliverEventArgs;

                        if (deliveryEventArgs == null) // poison
                        {
                            break;
                        }

                        byte[] body = deliveryEventArgs.Body;

                        var typeLen = BitConverter.ToInt16(body, 0);

                        //var typeDesc = Encoding.UTF8.GetString(body, 2, typeLen);

                        var messageType = registry.GetType(deliveryEventArgs.BasicProperties.Type);

                        if (messageType != null)
                        {
                            var serializedMessage = Encoding.UTF8.GetString(body, typeLen + 2, body.Length - typeLen - 2);
                            var messageObject = JsonConvert.DeserializeObject(serializedMessage, messageType);

                            Console.WriteLine(Thread.CurrentThread.ManagedThreadId);

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
            }, cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Stop()
        {
            if (consumer != null && consumer.IsRunning)
            {
                consumer.Queue.Enqueue(null);
            }

            if(cts != null)
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
