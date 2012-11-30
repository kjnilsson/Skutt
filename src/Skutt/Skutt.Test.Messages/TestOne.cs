using System;

namespace Skutt.Test.Messages
{
    public class TestOne
    {
        public Guid CorrelationId { get; set; }
        public DateTime SentOn { get; set; }
        public string Greeting { get; set; }
    }
}