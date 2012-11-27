using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Skutt.Contract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Skutt.RabbitMq
{
    internal class QueueSubscriber
    {
        private readonly string queue;
        private readonly IConnection connection;
        private readonly MessageTypeRegistry registry;
        private readonly Action<object> queueAdd;
        private QueueingBasicConsumer consumer;

        public QueueSubscriber(string queue,
            IConnection connection,
            MessageTypeRegistry registry,
            Action<object> handle)
        {
            Preconditions.Require(queue, "queue");
            Preconditions.Require(connection, "connection");
            Preconditions.Require(registry, "registry");
            Preconditions.Require(handle, "handle");

            this.queue = queue;
            this.connection = connection;
            this.registry = registry;
            this.queueAdd = handle;

            StartConsuming();
        }

        private void StartConsuming()
        {
            Task.Factory.StartNew(() =>
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ConfirmSelect();
                    consumer = new QueueingBasicConsumer(channel);
                    
                    channel.QueueDeclare(queue, true, false, false, null);
                    channel.BasicConsume(queue, false, consumer);
                    
                    while (true)
                    {
                        var deliveryEventArgs = consumer.Queue.Dequeue() as BasicDeliverEventArgs;

                        if(deliveryEventArgs == null) // poison
                        {
                            break;
                        }

                        byte[] body = deliveryEventArgs.Body;

                        var typeLen = BitConverter.ToInt16(body, 0);

                        var typeDesc = Encoding.UTF8.GetString(body, 2, typeLen);
                        
                        var messageType = registry.GetType(deliveryEventArgs.BasicProperties.Type);
                        
                        if (messageType != null)
                        {
                            var serializedMessage = Encoding.UTF8.GetString(body, typeLen + 2, body.Length - typeLen - 2);
                            var messageObject = JsonConvert.DeserializeObject(serializedMessage, messageType);
                            
                            queueAdd(messageObject);
                            channel.BasicAck(deliveryEventArgs.DeliveryTag, false);
                        }
                        else
                        {
                            //TODO handle dead letter queue
                            channel.BasicReject(deliveryEventArgs.DeliveryTag, false);
                        }
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public void Stop()
        {
            consumer.Queue.Enqueue(null);
        }
    }
}
