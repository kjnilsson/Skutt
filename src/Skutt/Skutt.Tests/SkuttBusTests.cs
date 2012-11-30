using Skutt.RabbitMq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Skutt.Tests
{
    public class SkuttBusTests
    {
        [Fact]
        public void Ctor_ShouldNotThrowIfUriProvided()
        {
            var sut = new SkuttBus(new Uri("test.test:3000"));
        }

        [Fact]
        public void Ctor_ShouldThrowIfNoUriProvided()
        {
            Assert.Throws<ArgumentException>(() => new SkuttBus(null));
        }
    }
}
