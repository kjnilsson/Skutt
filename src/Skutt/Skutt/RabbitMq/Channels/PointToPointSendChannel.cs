using System;
using RabbitMQ.Client;
using Skutt.Contract;

namespace Skutt.RabbitMq.Channels
{
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
}