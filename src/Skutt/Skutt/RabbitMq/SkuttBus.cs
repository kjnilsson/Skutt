using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Skutt.Contract;
using System.Reactive.Subjects;
using System.Threading;
using RabbitMQ.Client.Exceptions;

namespace Skutt.RabbitMq
{
    public class SkuttBus : IBus, IDisposable
    {
        private readonly IDictionary<string, QueueSubscriber> queueSubscribers = new Dictionary<string, QueueSubscriber>();
        private readonly BlockingCollection<object> commandQueue = new BlockingCollection<object>(50);
        private readonly IDictionary<Type, Action<object>> commandHandlers = new Dictionary<Type, Action<object>>();
        private readonly MessageTypeRegistry registry = new MessageTypeRegistry();

        private IRabbitMqChannelFactory channelFactory;

        private readonly Uri rabbitServer;
        private IConnection connection;

        public SkuttBus(Uri rabbitServer)
        {
            Preconditions.Require(rabbitServer, "rabbitServer"); 

            this.rabbitServer = rabbitServer;
        }

        private IRabbitMqChannelFactory ChannelFactory
        {
            get
            {
                if(connection == null || connection.IsOpen == false)
                {
                    throw new SkuttException("Cant use the channle factory unless the connection is open");
                }

                if(channelFactory == null)
                {
                    this.channelFactory = new RabbitMqChannelFactory(connection);
                }

                return channelFactory;
            }
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

            this.channelFactory = new RabbitMqChannelFactory(connection);

            StartCommandProcessor();
        }

        private void StartCommandProcessor()
        {
            Task.Factory.StartNew(() =>
                                      {
                                          foreach (var message in commandQueue.GetConsumingEnumerable())
                                          {
                                              Action<object> handler;

                                              if (commandHandlers.TryGetValue(message.GetType(), out handler))
                                              {
                                                  try
                                                  {
                                                      handler(message);
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
                using(var channel = ChannelFactory.PointToPointSend(destination))
                {
                    channel.Put(MessageSerializer.SerializeDefault(command, messageTypeUri), messageTypeUri.ToString());
                }
            }
            catch (OperationInterruptedException oie)
            {
                throw new SkuttException(
                    string.Format("Queue: {0} does not exist so you can't send a command to it", destination), oie);
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public void Receive<TCommand>(string queue, Action<TCommand> handler)
        {
            Preconditions.Require(queue, "queue");
            Preconditions.Require(handler, "handler");

            if (queueSubscribers.ContainsKey(queue) == false)
            {
                queueSubscribers.Add(queue, new QueueSubscriber(queue, connection, registry, o => commandQueue.Add(o), true));
            }

            commandHandlers.Add(typeof(TCommand), o => handler((dynamic)o));
        }

        public void Dispose()
        {
            foreach (var commandSubscriber in queueSubscribers.Values)
            {
                commandSubscriber.Stop();
                commandSubscriber.Dispose();
            }

            if (connection != null && connection.IsOpen)
            {
                connection.Close();
            }

            GC.SuppressFinalize(this);
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

            using(var channel = ChannelFactory.PublishTopic(topic))
            {
                channel.Put(MessageSerializer.SerializeDefault(@event, messageTypeUri), messageTypeUri.ToString());
            }
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
            queueSubscribers.Add(queue, 
                new QueueSubscriber(queue, connection, registry, o => subject.OnNext((dynamic)o), true));
        }
    }
}
