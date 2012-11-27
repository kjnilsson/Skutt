using Skutt.RabbitMq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Skutt.Tests
{
    public class MessageTypeRegistryTest
    {
        class TestMessage
        { }
        
        class TestMessage2
        { }

        [Fact]
        public void AddedMappingShouldBeRetrievableAsFromType()
        {
            //Arrange
            var sut = new MessageTypeRegistry();
            var uri = new Uri("http://messages.skutt.net/tests");
            
            //Act
            sut.Add<TestMessage>(uri);

            //Assert
            Assert.Equal(uri, sut.GetUri<TestMessage>());
            Assert.Equal(typeof(TestMessage), sut.GetType(uri));
            Assert.Equal(typeof(TestMessage), sut.GetType("http://messages.skutt.net/tests"));
        }

        [Fact]
        public void AddShouldThrowIfUriHasBeenRegisteredToAnotherType()
        {
            var sut = new MessageTypeRegistry();
            var uri = new Uri("http://messages.skutt.net/tests");
            sut.Add<TestMessage>(uri);

            Assert.Throws<SkuttException>(() => sut.Add<TestMessage2>(uri));
        }

        [Fact]
        public void AddShouldThrowIfTypeHasBeenRegisteredAgainstAnotherUri()
        {
            var sut = new MessageTypeRegistry();
            var uri = new Uri("http://messages.skutt.net/test_message");
            sut.Add<TestMessage>(uri);

            Assert.Throws<SkuttException>(() => sut.Add<TestMessage>(new Uri("http://messages.skutt.net/test_message2")));
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
