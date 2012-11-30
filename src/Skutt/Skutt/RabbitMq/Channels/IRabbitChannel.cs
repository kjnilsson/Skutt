using System;

namespace Skutt.RabbitMq
{
    public interface IRabbitChannel : IDisposable
    {
        void Put(byte[] message, string typeHeader);
    }
}