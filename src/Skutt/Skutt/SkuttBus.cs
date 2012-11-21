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
using System.Reactive.Subjects;
using Skutt.RabbitMq;

namespace Skutt
{
    public class SkuttBus : IBus, IDisposable
    {
        private readonly IDictionary<Type, MessageType> messageTypes = new Dictionary<Type, MessageType>();
        private readonly IDictionary<string, QueueSubscriber> commandSubscribers = new Dictionary<string, QueueSubscriber>();
        private readonly IDictionary<string, QueueSubscriber> eventSubscribers = new Dictionary<string, QueueSubscriber>();

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
                                                  try
                                                  {
                                                      handlers[message.GetType()]((dynamic)message);
                                                  }
                                                  catch (Exception e)
                                                  { 
                                                    //TODO send to error queue if configured
                                                  }
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

                var msgSerialized = payLoad.Concat(Encoding.UTF8.GetBytes(message)).ToArray();

                var bp = channel.CreateBasicProperties();
                bp.ContentType = "application/skutt";
                bp.SetPersistent(true);
                bp.CorrelationId = Guid.NewGuid().ToString();
                bp.Type = messageType;

                //this will throw an error if the queue does nto exisit ,i.e. there are no potential handlers
                channel.QueueDeclarePassive(destination);
                channel.BasicPublish(string.Empty, destination, bp, msgSerialized);
            }
        }

        public void Receive<TCommand>(string queue, Action<TCommand> handler)
        {
            Preconditions.Require(queue, "queue");
            Preconditions.Require(handler, "handler");

            if(commandSubscribers.ContainsKey(queue) == false)
            {
                commandSubscribers.Add(queue, new QueueSubscriber(queue, connection, messageTypes, o => commandQueue.Add(o)));
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

        public void Publish<TEvent>(TEvent @event)
        {
            if (messageTypes.ContainsKey(typeof(TEvent)) == false)
            {
                throw new ArgumentException(string.Format("The type: {0} has not been registered with the bus", typeof(TEvent).Name));
            }

            //create exchange
            using (var channel = connection.CreateModel())
            {
                var mt = messageTypes[typeof(TEvent)].Type;
                var exchangeName = GetExchangeName(mt);
                channel.ExchangeDeclare(exchangeName, "topic");
                //var routingKey = exchangeName + "." + subscriptionId;
                var bp = channel.CreateBasicProperties();
                bp.SetPersistent(true);

                channel.BasicPublish(exchangeName, "#", false, bp, SerializeMessage(@event, mt));
            }
        }

        private byte[] SerializeMessage(object message, Uri mt)
        {
            var lenBytes = BitConverter.GetBytes((short)mt.ToString().Length);

                var typeBytes = Encoding.UTF8.GetBytes(mt.ToString());

                var payLoad = lenBytes.Concat(typeBytes).ToArray();

                var json = JsonConvert.SerializeObject(message);

                return payLoad.Concat(Encoding.UTF8.GetBytes(json)).ToArray();
        }

        public IObservable<TEvent> Subscribe<TEvent>(string subscriptionId)
        {
            //create and queuea from message namespace
            //create binding 
            if (messageTypes.ContainsKey(typeof(TEvent)) == false)
            {
                throw new ArgumentException(string.Format("The type: {0} has not been registered with the bus", typeof(TEvent).Name));
            }

            var mt = messageTypes[typeof(TEvent)].Type;

            var exchangeName = GetExchangeName(mt);
            var routingKey = exchangeName + "." + subscriptionId;
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchangeName, "topic");
                channel.QueueDeclare(routingKey, true, false, false, null);
                channel.QueueBind(routingKey, exchangeName, "#");
            }

            var subject = new Subject<TEvent>();
            
            StartEventSubscriber(subject, routingKey);
            
            return subject;
        }

        private static string GetExchangeName(Uri mt)
        {
            var exchangeName = string.Concat(mt.Authority, mt.LocalPath.Replace('/', '.'));
            return exchangeName;
        }

        private void StartEventSubscriber<TEvent>(Subject<TEvent> subject, string queue)
        {
            Task.Factory.StartNew(() =>
                { 
                    using(var channel = connection.CreateModel())
                    {
                        var qs = new QueueSubscriber(queue, connection, messageTypes, o => subject.OnNext((dynamic)o));
                        eventSubscribers.Add(queue, qs);
                    }
                }, TaskCreationOptions.LongRunning);
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
