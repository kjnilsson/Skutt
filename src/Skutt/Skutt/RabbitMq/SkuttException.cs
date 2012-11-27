using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Skutt.RabbitMq
{
    public class SkuttException : Exception
    {
        public SkuttException(string message) : base(message)
        {
        }

        public SkuttException(string message, Exception exception): base(message, exception)
        {
        }
    }
}
