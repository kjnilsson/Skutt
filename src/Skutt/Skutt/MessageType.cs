using Skutt.Contract;
using Skutt.RabbitMq;
using System;
using System.Collections.Generic;

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

    public class MessageTypeRegistry
    {
        IDictionary<Type, Uri> typesToUris = new Dictionary<Type, Uri>();
        IDictionary<Uri, Type> urisToTypes = new Dictionary<Uri, Type>();

        public void Add<TMessage>(Uri uri)
        {
            Preconditions.Require(uri, "uri");

            if (typesToUris.ContainsKey(typeof(TMessage)) == false)
            {
                typesToUris.Add(typeof(TMessage), uri);
            }

            if (urisToTypes.ContainsKey(uri) == false)
            {
                urisToTypes.Add(uri, typeof(TMessage));
            }
        }

        public Uri GetUri<TMessage>()
        {
            Uri uri;
            typesToUris.TryGetValue(typeof(TMessage), out uri);

            if (uri == default(Uri))
            { 
                throw new SkuttException(string.Format("The message type: {0} was not registered with the bus", typeof(TMessage).Name));
            }

            return uri;
        }

        public Type GetType(Uri uri)
        {
            Preconditions.Require(uri, "uri");

            Type type;
            urisToTypes.TryGetValue(uri, out type);

            if (type == default(Type))
            {
                throw new SkuttException(string.Format("The message type uri: {0} was not registered with the bus", uri));
            }

            return type;
        }

        public Type GetType(string uri)
        {
            Preconditions.Require(uri, "uri");
            
            return this.GetType(new Uri(uri));
        }
    }
}