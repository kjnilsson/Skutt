using System;
using System.Linq;
using System.Text;
using Newtonsoft.Json;

namespace Skutt
{
    public class MessageSerializer
    {
        public static byte[] SerializeDefault(object message, Uri messageTypeUri)
        {
            var type = messageTypeUri.ToString();
            var lenBytes = BitConverter.GetBytes((short)type.Length);
            var typeBytes = Encoding.UTF8.GetBytes(type);
            var payLoad = lenBytes.Concat(typeBytes).ToArray();
            var json = JsonConvert.SerializeObject(message);

            return payLoad.Concat(Encoding.UTF8.GetBytes(json)).ToArray();
        }
    }
}