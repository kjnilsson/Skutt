using System;
using Skutt.Contract;

namespace Skutt.RabbitMq.Extensions
{
    public static class UriExtensions
    {
         public static string ToExchangeName(this Uri uri)
         {
             Preconditions.Require(uri, "uri");

             return string.Concat(uri.Authority, uri.LocalPath.Replace('/', '.'));
         }
    }
}