using System;
using RabbitMQ.Client;
using Skutt.Contract;

namespace Skutt.RabbitMq.Channels
{
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
}