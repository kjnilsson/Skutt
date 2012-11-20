using System;

namespace Skutt.Contract
{
    public class PreconditionFailedException : Exception
    {
        public PreconditionFailedException(string message) : base(message)
        {
        }
    }
}