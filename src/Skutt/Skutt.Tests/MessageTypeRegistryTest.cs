using Skutt.RabbitMq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Skutt.Tests
{
    class MessageTypeRegistryTest
    {
        class TestMessage
        { }

        [Fact]
        public void AddedMappingShouldBeRetrievableAsFromType()
        {
            var sut = new MessageTypeRegistry();
            var uri = new Uri("http://messages.skutt.net/tests");
            sut.Add<TestMessage>(uri);

            Assert.Equal(uri, sut.GetUri<TestMessage>());
            Assert.Equal(typeof(TestMessage), sut.GetType(uri));
            Assert.Equal(typeof(TestMessage), sut.GetType("http://messages.skutt.net/tests"));
        }

        [Fact]
        public void GetUri_ShouldThrowWhenNoRegistryAdded()
        {
            var sut = new MessageTypeRegistry();

            Assert.Throws<SkuttException>(() => sut.GetUri<TestMessage>());
        }

        [Fact]
        public void GetType_ShouldThrowWhenNoRegistryAdded()
        {
            var sut = new MessageTypeRegistry();

            Assert.Throws<SkuttException>(() => sut.GetType(new Uri("http://www.google.com")));
        }

        [Fact]
        public void GetType_ShouldFailPreconditionsIfPassedNullUri()
        {
            var sut = new MessageTypeRegistry();
            Uri uri = null;
            Assert.Throws<ArgumentException>(() => sut.GetType(uri));
        }
    }
}
