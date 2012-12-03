using System;
using Xunit;
using Skutt.Extensions;

namespace Skutt.Tests
{
    public class UriExtensionsTests
    {
        [Fact]
        public void ToExchangeNameShouldCorrectlyFormatExchangeName()
        {
            var sut = new Uri("http://api.skutt.net/messages/events/someevent");

            var result = sut.ToExchangeName();

            Assert.Equal("api.skutt.net.messages.events.someevent", result);
        }

        [Fact]
        public void ToExchangeName_ShouldThrowIfGivenNullUri()
        {
            Uri sut = null;

            Assert.Throws<ArgumentException>(() => sut.ToExchangeName());
        }
    }
}