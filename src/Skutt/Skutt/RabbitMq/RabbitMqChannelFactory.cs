using System;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Skutt.Contract;

namespace Skutt.RabbitMq
{
    public interface IRabbitMqChannelFactory
    {
        IRabbitChannel PointToPointSend(string queue);
        IRabbitChannel PublishTopic(string topic);
    }

    public class RabbitMqChannelFactory : IRabbitMqChannelFactory
    {
        private readonly IConnection connection;

        public RabbitMqChannelFactory(IConnection connection)
        {
            this.connection = connection;
        }

        public IRabbitChannel PointToPointSend(string queue)
        {
            return new PointToPointSendChannel(connection, queue);
        }

        public IRabbitChannel PublishTopic(string topic)
        {
            return new PublishTopicChannel(connection, topic);
        }
    }

    public class PublishTopicChannel : IRabbitChannel
    {
        private readonly IModel channel;
        private readonly string topic;

        public PublishTopicChannel(IConnection connection, string topic)
        {
            this.channel = connection.CreateModel();
            this.topic = topic;
        }

        public void Put(byte[] message, string typeHeader)
        {
            Preconditions.Require(message, "message");
            Preconditions.Require(typeHeader, "typeHeader");

            var exchangeName = GetExchangeName(new Uri(typeHeader));
            channel.ExchangeDeclare(exchangeName, "topic");
            var basicProperties = GetBasicProperties(typeHeader, channel);

            channel.BasicPublish(exchangeName, topic, false, basicProperties, message);
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

        private static string GetExchangeName(Uri mt)
        {
            var exchangeName = string.Concat(mt.Authority, mt.LocalPath.Replace('/', '.'));
            return exchangeName.ToLower();
        }

        public void Dispose()
        {
            this.channel.Dispose();
        }
    }

    public class PointToPointSendChannel : IRabbitChannel
    {
        private readonly IModel channel;
        private readonly string queue;

        public PointToPointSendChannel(IConnection connection, string queue)
        {
            this.channel = connection.CreateModel();
            this.queue = queue;
        }

        public void Put(byte[] message, string typeHeader)
        {
            Preconditions.Require(message, "message");
            Preconditions.Require(typeHeader, "typeHeader");

            channel.QueueDeclarePassive(queue);
            channel.BasicPublish(string.Empty, queue, GetBasicProperties(typeHeader, channel), message);
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

        public void Dispose()
        {
            this.channel.Dispose();
        }
    }

    public interface IRabbitChannel : IDisposable
    {
        void Put(byte[] message, string typeHeader);
    }
}