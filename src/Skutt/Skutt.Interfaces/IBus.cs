using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

// ReSharper disable CheckNamespace
namespace Skutt
// ReSharper restore CheckNamespace
{
    public interface IBus
    {
        void Send<TCommand>(string destination, TCommand command);

        void Receive<TCommand>(string queue, Action<TCommand> handler);

        void Publish<TEvent>(TEvent @event);

        IObservable<TEvent> Subscribe<TEvent>(string subscriptionId);
    }
}
