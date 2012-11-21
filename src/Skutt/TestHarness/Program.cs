using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Skutt;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Skutt.RabbitMq;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            var bus = new SkuttBus(new Uri(@"amqp://guest:guest@localhost:5672"));
            bus.Connect();
            bus.RegisterMessageType<TestOne>(new Uri("http://msg.skutt.net/messages/test_one"));
            bus.RegisterMessageType<DeadLetter>(new Uri("http://msg.skutt.net/messages/dead_letter"));

            bus.Receive<TestOne>("skutt_object", m => Console.WriteLine(m.CorrelationId) );

            SendCommands(bus);
            
            var s = bus.Subscribe<TestOne>("my_test");
            var _ = s.Where(m => m.Greeting.Equals("hello"))
                     .Subscribe(m => Console.WriteLine(m.Greeting));

            bus.Publish<TestOne>(new TestOne() { CorrelationId = Guid.NewGuid(), Greeting = "hello" });
            bus.Publish<TestOne>(new TestOne() { CorrelationId = Guid.NewGuid(), Greeting = "bye" });

            Console.ReadKey();
            bus.Dispose();
        }

        public static void SendCommands(IBus bus)
        {

            
            
            bus.Send<DeadLetter>("skutt_object", new DeadLetter { });
            bus.Send<TestOne>("skutt_object", new TestOne { CorrelationId = Guid.NewGuid() });
            //bus.Dispose();
        }
    }

    public class TestOne
    {
        public Guid CorrelationId { get; set; }
        public DateTime SentOn { get; set; }
        public string Greeting { get; set; }
    }

    public class DeadLetter
    { }
}
