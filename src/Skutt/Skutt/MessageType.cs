using System;

namespace Skutt
{
    public class MessageType
    {
        public MessageType(Uri type, Type clrType)
        {
            ClrType = clrType;
            Type = type;
        }

        public Uri Type { get; private set; }
        public Type ClrType { get; private set; }
    }
}