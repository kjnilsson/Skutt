using System;
using Skutt;
using System.Reactive.Linq;
using Skutt.RabbitMq;
using System.Threading;
using Skutt.Test.Messages;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            var bus = new SkuttBus(new Uri(@"amqp://guest:guest@localhost:5672"));
            bus.Connect();
            bus.RegisterMessageType<TestCommandOne>(new Uri("http://msg.skutt.net/messages/test_one"));
            bus.RegisterMessageType<TestEventOne>(new Uri("http://msg.skutt.net/messages/test_two"));
            bus.RegisterMessageType<DeadLetter>(new Uri("http://msg.skutt.net/messages/dead_letter"));

            bus.Receive<TestCommandOne>("skutt_object", m => Console.WriteLine("Command: " + m.CorrelationId + " " + Thread.CurrentThread.ManagedThreadId.ToString()));

            bus.Send("skutt_object", new TestCommandOne { CorrelationId = Guid.NewGuid() });
            
            Console.WriteLine("App thread: " + Thread.CurrentThread.ManagedThreadId.ToString());
            //Thread.Sleep(1000);

            //bus.Subscribe<TestCommandOne>("sub_test", m => Console.WriteLine("Event: TestCommandOne" + m.Greeting));


            var obs = bus.Observe<TestEventOne>("my_test");
            var obs2 = bus.Observe<TestEventOne>("my_test2").Subscribe(e => Console.WriteLine("Event: TestEventOne" + Thread.CurrentThread.ManagedThreadId.ToString()));

            var __ = obs.Subscribe(m => Console.WriteLine("Event: TestEventOne" + m.Greeting + Thread.CurrentThread.ManagedThreadId.ToString()));


            //var _ = obs.Where(m => m.Greeting.Equals("hello"))
            //         .Subscribe(m =>
            //             {
            //                 Console.WriteLine("Event: " + m.Greeting + Thread.CurrentThread.ManagedThreadId.ToString());
            //                 bus.Publish(new TestEventOne());
            //                 bus.Publish(new TestEventOne());
            //                 bus.Publish(new TestEventOne());
            //                 bus.Publish(new TestEventOne());
            //                 bus.Publish(new TestEventOne());
            //                 bus.Publish(new TestEventOne());
            //                 bus.Publish(new TestEventOne());
            //             });

            bus.Publish(new TestEventOne { CorrelationId = Guid.NewGuid(), Greeting = "hello" });
            bus.Publish(new TestEventOne { CorrelationId = Guid.NewGuid(), Greeting = "bye" });

            Console.ReadKey();
            bus.Dispose();
        }
    }

    public class DeadLetter
    { }
}
