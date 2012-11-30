using RabbitMQ.Client;
using Skutt.RabbitMq.Channels;

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
}