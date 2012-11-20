using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Skutt;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            var bus = new SkuttBus(new Uri(@"amqp://guest:guest@localhost:5672"));
            bus.Connect();
            bus.RegisterMessageType<TestOne>(new Uri("http://msg.skutt.net/messages/test_one"));

            bus.Receive<TestOne>("skutt_object", m => Console.WriteLine(m.CorrelationId) );

            bus.Send<TestOne>("skutt_object", new TestOne { CorrelationId = Guid.NewGuid() });
        }
    }

    public class TestOne
    {
        public Guid CorrelationId { get; set; }
        public DateTime SentOn { get; set; }
        public string Greeting { get; set; }
    }
}
