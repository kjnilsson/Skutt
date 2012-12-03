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
            bus.RegisterMessageType<TestOne>(new Uri("http://msg.skutt.net/messages/test_one"));
            bus.RegisterMessageType<TestTwo>(new Uri("http://msg.skutt.net/messages/test_two"));
            bus.RegisterMessageType<DeadLetter>(new Uri("http://msg.skutt.net/messages/dead_letter"));

            bus.Receive<TestOne>("skutt_object", m => Console.WriteLine("Command: " + m.CorrelationId + " " + Thread.CurrentThread.ManagedThreadId.ToString()));

            SendCommands(bus);
            
            Console.WriteLine("App thread: " + Thread.CurrentThread.ManagedThreadId.ToString());
            Thread.Sleep(1000);

            //bus.Subscribe<TestOne>("sub_test", m => Console.WriteLine("Event: TestOne" + m.Greeting));

            
            var obs = bus.Observe<TestOne>("my_test");
            var obs2 = bus.Observe<TestTwo>("my_test").Subscribe(e => Console.WriteLine("Event: TestTwo" + Thread.CurrentThread.ManagedThreadId.ToString()));

            var __ = obs.Subscribe(m => Console.WriteLine("Event: TestTwo" + m.Greeting + Thread.CurrentThread.ManagedThreadId.ToString()));


            //var _ = obs.Where(m => m.Greeting.Equals("hello"))
            //         .Subscribe(m =>
            //             {
            //                 Console.WriteLine("Event: " + m.Greeting + Thread.CurrentThread.ManagedThreadId.ToString());
            //                 bus.Publish(new TestTwo());
            //                 bus.Publish(new TestTwo());
            //                 bus.Publish(new TestTwo());
            //                 bus.Publish(new TestTwo());
            //                 bus.Publish(new TestTwo());
            //                 bus.Publish(new TestTwo());
            //                 bus.Publish(new TestTwo());
            //             });

            bus.Publish(new TestOne { CorrelationId = Guid.NewGuid(), Greeting = "hello" });
            bus.Publish(new TestOne { CorrelationId = Guid.NewGuid(), Greeting = "bye" });

            Console.ReadKey();
            bus.Dispose();
        }

        public static void SendCommands(IBus bus)
        {
            //bus.Send("skutt_object", new DeadLetter { });
            bus.Send("skutt_object", new TestOne { CorrelationId = Guid.NewGuid() });
        }
    }

    public class DeadLetter
    { }
}
