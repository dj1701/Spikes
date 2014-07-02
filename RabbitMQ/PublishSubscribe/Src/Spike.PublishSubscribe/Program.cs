using System;
using System.Net.Sockets;
using RabbitMQ.Client;

namespace Spike.Publish
{
    public interface IPublish
    {
        void SendToFanOutExchange();
        void SendToDirectExchange();
    }

    public class MessagingPublish : IPublish
    {
        private readonly ConnectionFactory _factory = new ConnectionFactory();
        private const string _routingKey = "123";
        private const string _fanoutExchange = "FanOut-Exchange";
        private const string _directExchange = "Direct-Exchange";
        private const string _fanoutQueue = "QueueForFanOutExchange";
        private const string _directQueue = "QueueForDirectExchange";

        public MessagingPublish()
        {
            //_factory.Uri = "amqp://user:pass@hostName:port/vhost";
            _factory.Uri = "amqp://localhost";
            _factory.Protocol = Protocols.DefaultProtocol;
            _factory.Port = AmqpTcpEndpoint.UseDefaultPort;
        }

        public void SendToFanOutExchange() 
        {
            using (IConnection conn = _factory.CreateConnection())
            using (IModel channel = conn.CreateModel())
            {
                channel.QueueDeclare(_fanoutQueue, true,false,false,null);
                channel.ExchangeDeclare(_fanoutExchange, "fanout");
                channel.QueueBind(_fanoutQueue, _fanoutExchange, "");

                const string message = "Hello Motor Team!!  This message is sent through a fanout exchange.";
                var encodedMessage = System.Text.Encoding.Unicode.GetBytes(message);
                channel.BasicPublish(_fanoutExchange, "", null, encodedMessage);
            }
        }

        public void SendToDirectExchange()
        {
            using (IConnection conn = _factory.CreateConnection())
            using (IModel channel = conn.CreateModel())
            {
                channel.QueueDeclare(_directQueue, true, false, false, null);
                channel.ExchangeDeclare(_directExchange, "direct");
                channel.QueueBind(_directQueue, _directExchange, _routingKey);

                const string message = "Hello Motor Team!!  This message is sent through a direct exchange.";
                var encodedMessage = System.Text.Encoding.Unicode.GetBytes(message);
                channel.BasicPublish(_directExchange, _routingKey, null, encodedMessage);
            }
        }
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            var publish = new MessagingPublish();

            Console.WriteLine("");
            Console.WriteLine("Main Menu (Send Messages)");
            Console.WriteLine("==========================");
            Console.WriteLine("1. Send Fan Out Exchange Message");
            Console.WriteLine("2. Send Direct Exchange Message");
            Console.WriteLine("3. Exit");
            Console.WriteLine("Enter your choice: ");
            var input = Console.ReadLine();
            int choice = 0;

            if (!int.TryParse(input, out choice)) return;
            switch (choice)
            {
                case 1:
                    publish.SendToFanOutExchange();
                    Console.Write("Message sent to Fan Out Exchange");
                    Console.ReadKey();
                    break;
                case 2:
                    publish.SendToDirectExchange();
                    Console.Write("Message received from Direct Exchange");
                    Console.ReadKey();
                    break;
                default:
                    Environment.Exit(0);
                    break;
            }
        }
    }
}
