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

namespace Skutt.RabbitMq
{
    public class SkuttBus : IBus, IDisposable
    {
        private readonly IDictionary<Type, MessageType> typeMap = new Dictionary<Type, MessageType>();
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
                                      }, TaskCreationOptions.LongRunning);
        }

        public void RegisterMessageType<TMessage>(Uri messageTypeId)
        {
            Preconditions.Require(messageTypeId, "messageType");

            this.typeMap.Add(typeof (TMessage), new MessageType(messageTypeId, typeof (TMessage)));
        }

        public void Send<TCommand>(string destination, TCommand command)
        {
            Preconditions.Require(destination, "destination");
            Preconditions.Require(command, "command");

            MessageType mt = GetMessageType<TCommand>();

            using(var channel = connection.CreateModel())
            {
                var msgSerialized = SerializeMessage(command, mt.TypeUri);

                var bp = channel.CreateBasicProperties();
                bp.ContentType = "application/skutt";
                bp.SetPersistent(true);
                bp.CorrelationId = Guid.NewGuid().ToString();
                bp.Type = mt.TypeUri.ToString();

                //this will throw an error if the queue does nto exisit ,i.e. there are no potential subscribers
                channel.QueueDeclarePassive(destination);
                channel.BasicPublish(string.Empty, destination, bp, msgSerialized);
            }
        }

        public void Receive<TCommand>(string queue, Action<TCommand> handler)
        {
            Preconditions.Require(queue, "queue");
            Preconditions.Require(handler, "handler");

            if (commandSubscribers.ContainsKey(queue) == false)
            {
                commandSubscribers.Add(queue, new QueueSubscriber(queue, connection, typeMap, o => commandQueue.Add(o)));
            }

            handlers.Add(typeof(TCommand), o => handler((dynamic)o));
        }

        public void Dispose()
        {
            foreach (var commandSubscriber in commandSubscribers.Values)
            {
                commandSubscriber.Stop();
            }

            foreach (var commandSubscriber in eventSubscribers.Values)
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

        public void Publish<TEvent>(TEvent @event)
        {
            Preconditions.Require(@event, "event");

            MessageType mt = GetMessageType<TEvent>();

            using (var channel = connection.CreateModel())
            {
                var exchangeName = GetExchangeName(mt.TypeUri);
                channel.ExchangeDeclare(exchangeName, "topic");
                //var routingKey = exchangeName + "." + subscriptionId;
                var bp = channel.CreateBasicProperties();
                bp.SetPersistent(true);

                channel.BasicPublish(exchangeName, "#", false, bp, SerializeMessage(@event, mt.TypeUri));
            }
        }

        private byte[] SerializeMessage(object message, Uri mt)
        {
            var lenBytes = BitConverter.GetBytes((short) mt.ToString().Length);

            var typeBytes = Encoding.UTF8.GetBytes(mt.ToString());

            var payLoad = lenBytes.Concat(typeBytes).ToArray();

            var json = JsonConvert.SerializeObject(message);

            return payLoad.Concat(Encoding.UTF8.GetBytes(json)).ToArray();
        }

        public IObservable<TEvent> Observe<TEvent>(string subscriptionId, string topic = "#")
        {
            Preconditions.Require(subscriptionId, "subscriptionId");
            //create and queuea from message namespace
            //create binding 
            MessageType mt = GetMessageType<TEvent>();

            var exchangeName = GetExchangeName(mt.TypeUri);
            var routingKey = exchangeName + "." + subscriptionId;

            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchangeName, "topic");
                channel.QueueDeclare(routingKey, true, false, false, null);
                channel.QueueBind(routingKey, exchangeName, topic);
            }

            var subject = new Subject<TEvent>();
            
            StartEventSubscriber(subject, routingKey);
            
            return subject;
        }

        private MessageType GetMessageType<TMessage>()
        {
            MessageType mt;
            if (typeMap.TryGetValue(typeof(TMessage), out mt) == false)
            {
                throw new ArgumentException(string.Format("The type: {0} has not been registered with the bus", typeof(TMessage).Name));
            }

            return mt;
        }

        private static string GetExchangeName(Uri mt)
        {
            var exchangeName = string.Concat(mt.Authority, mt.LocalPath.Replace('/', '.'));
            return exchangeName.ToLower();
        }

        private void StartEventSubscriber<TEvent>(IObserver<TEvent> subject, string queue)
        {
            Task.Factory.StartNew(() =>
                                      {
                                          var qs = new QueueSubscriber(queue, connection, typeMap, o => subject.OnNext((dynamic) o));
                                          eventSubscribers.Add(queue, qs);
                                          //Console.WriteLine("event subscriber thread: " + Thread.CurrentThread.ManagedThreadId);

                                      }, TaskCreationOptions.LongRunning);
        }
    }
}
