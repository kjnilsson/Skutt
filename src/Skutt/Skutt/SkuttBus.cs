using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Skutt.Contract;

namespace Skutt
{
    public class SkuttBus : IBus, IDisposable
    {
        private readonly IDictionary<Type, MessageType> messageTypes = new Dictionary<Type, MessageType>();
        private readonly IDictionary<string, QueueSubscriber> listeners = new Dictionary<string, QueueSubscriber>();

        private readonly BlockingCollection<object> commandQueue = new BlockingCollection<object>();

        private readonly IDictionary<Type, Action<object>> handlers = new Dictionary<Type, Action<object>>();

        private readonly Uri rabbitServer;
        private IConnection connection;

        public SkuttBus(Uri rabbitServer)
        {
            Preconditions.Require(rabbitServer, "rabbitServer");
            this.rabbitServer = rabbitServer;
        }

        public void Connect()
        {
            var cf = new ConnectionFactory
                         {
                             Uri = rabbitServer.ToString()
                         };

            this.connection = cf.CreateConnection();

            StartCommandProcessor();
        }

        private void StartCommandProcessor()
        {
            Task.Factory.StartNew(() =>
                                      {
                                          foreach (var message in commandQueue.GetConsumingEnumerable())
                                          {
                                              if (handlers.ContainsKey(message.GetType()))
                                              {
                                                  handlers[message.GetType()]((dynamic) message);
                                              }
                                          }
                                      });
        }

        public void RegisterMessageType<TMessage>(Uri messageType)
        {
            this.messageTypes.Add(typeof (TMessage), new MessageType(messageType, typeof (TMessage)));
        }

        public void Send<TCommand>(string destination, TCommand command)
        {
            if(messageTypes.ContainsKey(typeof(TCommand)) == false)
            {
                throw new ArgumentException(string.Format("The type: {0} has not been registered with the bus", command));
            }

            using(var channel = connection.CreateModel())
            {
                var messageType = messageTypes[typeof(TCommand)].Type.ToString();

                var lenBytes = BitConverter.GetBytes((short)messageType.Length);

                var typeBytes = Encoding.UTF8.GetBytes(messageType);

                var payLoad = lenBytes.Concat(typeBytes).ToArray();

                var message = JsonConvert.SerializeObject(command);

                var bp = channel.CreateBasicProperties();
                bp.ContentType = "application/skutt";
                bp.SetPersistent(true);
                bp.CorrelationId = Guid.NewGuid().ToString();
                bp.Type = messageType;

                channel.BasicPublish(string.Empty, destination, bp, payLoad.Concat(Encoding.UTF8.GetBytes(message)).ToArray());
            }
        }

        public void Receive<TCommand>(string queue, Action<TCommand> handler)
        {
            if(listeners.ContainsKey(queue) == false)
            {
                listeners.Add(queue, new QueueSubscriber(queue, connection, messageTypes, commandQueue));
            }

            handlers.Add(typeof(TCommand), o => handler((dynamic)o));
        }

        public void Dispose()
        {
            connection.Close();
        }

        ~SkuttBus()
        {
            if (connection != null && connection.IsOpen)
            {
                connection.Close();
            }
        }
    }

    internal class QueueSubscriber
    {
        private readonly string queue;
        private readonly IConnection connection;
        private readonly IDictionary<Type, MessageType> messageTypes;
        private readonly BlockingCollection<object> commandQueue;

        public QueueSubscriber(string queue, IConnection connection, IDictionary<Type, MessageType> messageTypes, BlockingCollection<object> commandQueue)
        {
            this.queue = queue;
            this.connection = connection;
            this.messageTypes = messageTypes;
            this.commandQueue = commandQueue;
            StartConsuming();
        }

        private void StartConsuming()
        {
            Task.Factory.StartNew(() =>
                                      {
                                          using (var channel = connection.CreateModel())
                                          {
                                              channel.ConfirmSelect();
                                              var consumer = new QueueingBasicConsumer(channel);

                                              channel.QueueDeclare(queue, true, false, false, null);
                                              channel.BasicConsume(queue, false, consumer);

                                              while (true)
                                              {
                                                  var ea = (BasicDeliverEventArgs) consumer.Queue.Dequeue();
                                                  byte[] body = ea.Body;

                                                  var typeLen = BitConverter.ToInt16(body, 0);

                                                  var typeDesc = Encoding.UTF8.GetString(body, 2, typeLen);
                                                  var mt =
                                                      messageTypes.Values.FirstOrDefault(
                                                          x => x.Type == new Uri(typeDesc));

                                                  if(mt != null)
                                                  {
                                                      var msg = Encoding.UTF8.GetString(body, typeLen + 2, body.Length - typeLen - 2);

                                                      var messageObject = JsonConvert.DeserializeObject(msg, mt.ClrType);

                                                      commandQueue.Add(messageObject);
                                                      channel.BasicAck(ea.DeliveryTag, false);
                                                  }
                                                  else
                                                  {
                                                      channel.BasicNack(ea.DeliveryTag, false, false);
                                                  }
                                              }
                                          }
                                      });
        }
    }

    public class MessageType
    {
        public MessageType(Uri type, Type clrType)
        {
            ClrType = clrType;
            Type = type;
        }

        public Uri Type { get; private set; }
        public Type ClrType { get; private set; }
    }
}
