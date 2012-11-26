using System;

namespace Skutt
{
    public class MessageType
    {
        public MessageType(Uri type, Type clrType)
        {
            ClrType = clrType;
            TypeUri = type;
        }

        public Uri TypeUri { get; private set; }
        public Type ClrType { get; private set; }
    }
}