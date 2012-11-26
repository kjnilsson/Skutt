using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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
        private readonly IDictionary<Type, MessageType> messageTypes;
        private readonly Action<object> queueAdd;
        private QueueingBasicConsumer consumer;

        public QueueSubscriber(string queue,
            IConnection connection,
            IDictionary<Type, MessageType> messageTypes,
            Action<object> queueAdd)
        {
            this.queue = queue;
            this.connection = connection;
            this.messageTypes = messageTypes;
            this.queueAdd = queueAdd;
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
                        var ea = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
                        if(ea == null) // poison
                        {
                            break;
                        }

                        byte[] body = ea.Body;

                        var typeLen = BitConverter.ToInt16(body, 0);

                        var typeDesc = Encoding.UTF8.GetString(body, 2, typeLen);
                        var mt = messageTypes.Values.FirstOrDefault(x => x.TypeUri == new Uri(typeDesc));

                        if (mt != null)
                        {
                            var msg = Encoding.UTF8.GetString(body, typeLen + 2, body.Length - typeLen - 2);
                            var messageObject = JsonConvert.DeserializeObject(msg, mt.ClrType);
                            
                            queueAdd(messageObject);
                            channel.BasicAck(ea.DeliveryTag, false);
                        }
                        else
                        {
                            //TODO handle dead letter queue
                            channel.BasicReject(ea.DeliveryTag, false);
                            //channel.BasicNack(ea.DeliveryTag, false, false);
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
