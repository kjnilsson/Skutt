using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Skutt.Contract;
using System.Reactive.Subjects;
using System.Threading;
using RabbitMQ.Client.Exceptions;

namespace Skutt.RabbitMq
{
    public class SkuttBus : IBus, IDisposable
    {
        //private readonly IDictionary<Type, MessageType> typeMap = new Dictionary<Type, MessageType>();
        private readonly IDictionary<string, QueueSubscriber> queueSubscribers = new Dictionary<string, QueueSubscriber>();
        private readonly BlockingCollection<object> commandQueue = new BlockingCollection<object>();
        private readonly IDictionary<Type, Action<object>> handlers = new Dictionary<Type, Action<object>>();
        private readonly MessageTypeRegistry registry = new MessageTypeRegistry();

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
            this.connection.ConnectionShutdown += (c, ea) => 
            { 
                Console.WriteLine("Connection interrupted"); 
            };

            StartCommandProcessor();
        }

        void connection_ConnectionShutdown(IConnection connection, ShutdownEventArgs reason)
        {
            throw new NotImplementedException();
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
                                      }, TaskCreationOptions.LongRunning);
        }

        public void RegisterMessageType<TMessage>(Uri messageTypeUri)
        {
            Preconditions.Require(messageTypeUri, "messageTypeUri");

            this.registry.Add<TMessage>(messageTypeUri);
        }

        public void Send<TCommand>(string destination, TCommand command)
        {
            Preconditions.Require(destination, "destination");
            Preconditions.Require(command, "command");

            var messageTypeUri = registry.GetUri<TCommand>();
            try
            {
                using (var channel = connection.CreateModel())
                {
                    var msgSerialized = SerializeMessage(command, messageTypeUri);
                    var bp = GetBasicProperties(messageTypeUri.ToString(), channel);

                    //this will throw an error if the queue does nto exist, i.e. there are no potential subscribers
                    channel.QueueDeclarePassive(destination);
                    channel.BasicPublish(string.Empty, destination, bp, msgSerialized);
                }
            }
            catch (OperationInterruptedException oie)
            {
                throw new SkuttException(
                    string.Format("Queue: {0} does not exist so you can't send a command to it", destination),
                    oie);
            }
            catch (Exception e)
            {
                throw;
            }
        }

        private static IBasicProperties GetBasicProperties(string messageUri, IModel channel)
        {
            var bp = channel.CreateBasicProperties();
            bp.ContentType = "application/skutt";
            bp.SetPersistent(true);
            bp.CorrelationId = Guid.NewGuid().ToString();
            bp.Type = messageUri;

            return bp;
        }

        public void Receive<TCommand>(string queue, Action<TCommand> handler)
        {
            Preconditions.Require(queue, "queue");
            Preconditions.Require(handler, "handler");

            if (queueSubscribers.ContainsKey(queue) == false)
            {
                queueSubscribers.Add(queue, new QueueSubscriber(queue, connection, registry, o => commandQueue.Add(o)));
            }

            handlers.Add(typeof(TCommand), o => handler((dynamic)o));
        }

        public void Dispose()
        {
            foreach (var commandSubscriber in queueSubscribers.Values)
            {
                commandSubscriber.Stop();
            }

            connection.Close();
        }

        ~SkuttBus()
        {
            if (connection != null && connection.IsOpen)
            {
                connection.Close();
            }
        }

        public void Publish<TEvent>(TEvent @event, string topic = "#")
        {
            Preconditions.Require(@event, "event");
            Preconditions.Require(topic, "topic");

            var messageTypeUri = registry.GetUri<TEvent>();

            using (var channel = connection.CreateModel())
            {
                var exchangeName = GetExchangeName(messageTypeUri);
                channel.ExchangeDeclare(exchangeName, "topic");
                var bp = GetBasicProperties(messageTypeUri.ToString(), channel);
                
                channel.BasicPublish(exchangeName, topic, false, bp, SerializeMessage(@event, messageTypeUri));
            }
        }

        private byte[] SerializeMessage(object message, Uri messageType)
        {
            // this is fairly horrid code
            var type = messageType.ToString();
            var lenBytes = BitConverter.GetBytes((short) type.Length);
            var typeBytes = Encoding.UTF8.GetBytes(type.ToString());
            var payLoad = lenBytes.Concat(typeBytes).ToArray();
            var json = JsonConvert.SerializeObject(message);

            return payLoad.Concat(Encoding.UTF8.GetBytes(json)).ToArray();
        }

        public IObservable<TEvent> Observe<TEvent>(string subscriptionId, string topic = "#")
        {
            Preconditions.Require(subscriptionId, "subscriptionId");
            Preconditions.Require(topic, "topic");

            var messageTypeUri = registry.GetUri<TEvent>();

            var exchangeName = GetExchangeName(messageTypeUri);
            var routingKey = exchangeName + "." + subscriptionId;

            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchangeName, "topic");
                channel.QueueDeclare(routingKey, true, false, false, null);
                channel.QueueBind(routingKey, exchangeName, topic);
            }

            var subject = new Subject<TEvent>();
            
            AddNewEventSubscriber(subject, routingKey);
            
            return subject;
        }

        private static string GetExchangeName(Uri mt)
        {
            var exchangeName = string.Concat(mt.Authority, mt.LocalPath.Replace('/', '.'));
            return exchangeName.ToLower();
        }

        private void AddNewEventSubscriber<TEvent>(IObserver<TEvent> subject, string queue)
        {
            queueSubscribers.Add(queue, new QueueSubscriber(queue, connection, registry, o => subject.OnNext((dynamic)o)));
        }
    }
}
