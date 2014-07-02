using System;
using RabbitMQ.Client;

namespace Spike.Subscribe
{
    class Program
    {
        public interface ISubscribe
        {
            string ReceiveFromFanOut(); 
            string ReceiveFromDirect();
        }

        public class MessageSubscribe : ISubscribe
        {
            private readonly ConnectionFactory _factory = new ConnectionFactory();
            private const string _fanoutExchange = "FanOut-Exchange";
            private const string _directExchange = "Direct-Exchange";
            private const string _fanoutQueue = "QueueForFanOutExchange";
            private const string _directQueue = "QueueForDirectExchange";

            public MessageSubscribe()
            {
                _factory.Uri = "amqp://localhost";
                _factory.Protocol = Protocols.DefaultProtocol;
                _factory.Port = AmqpTcpEndpoint.UseDefaultPort;
            }
            
            public string ReceiveFromDirect()
            {
                string receivedMessage;

                using (IConnection conn = _factory.CreateConnection())
                using (IModel channel = conn.CreateModel())
                {
                    channel.QueueDeclare(_directQueue, true, false, false, null);
                    channel.ExchangeDeclare(_directExchange, "direct");
                    channel.QueueBind(_directQueue, _fanoutExchange, "");

                    var receiveEncodedMessage = channel.BasicGet(_directQueue, false);
                    receivedMessage = System.Text.Encoding.Unicode.GetString(receiveEncodedMessage.Body);
                }

                return receivedMessage;
            }

            public string ReceiveFromFanOut()
            {
                string message;

                using (IConnection conn = _factory.CreateConnection())
                using (IModel channel = conn.CreateModel())
                {
                    channel.QueueDeclare(_fanoutQueue, true, false, false, null);
                    channel.ExchangeDeclare(_directExchange, "fanout");
                    channel.QueueBind(_fanoutQueue, _directExchange, "");

                    var encodedMessage = channel.BasicGet(_fanoutQueue, false);
                    message = System.Text.Encoding.Unicode.GetString(encodedMessage.Body);
                }

                return message;
            }
        }

        static void Main(string[] args)
        {
            var subscribe = new MessageSubscribe();

            Console.WriteLine("");
            Console.WriteLine("Main Menu (Receive Messages)");
            Console.WriteLine("==========================");
            Console.WriteLine("1. Receive from Fan Out Exchange");
            Console.WriteLine("2. Send Direct Exchange Message");
            Console.WriteLine("3. Exit");
            Console.WriteLine("Enter your choice: ");
            var input = Console.ReadLine();
            int choice = 0;

            if (!int.TryParse(input, out choice)) return;
            switch (choice)
            {
                case 1:
                    subscribe.ReceiveFromFanOut();
                    var receiveFromFanOut = subscribe.ReceiveFromFanOut();
                    Console.Write("Message received from Fan Out Exchange: \n\n{0}",receiveFromFanOut);
                    Console.Read();
                    break;
                case 2:
                    var receiveFromDirect = subscribe.ReceiveFromDirect();
                    Console.Write("Message received from Direct Exchange: \n\n{0}",receiveFromDirect);
                    Console.Read();
                    break;
                default:
                    Environment.Exit(0);
                    break;
            }
        }
    }
}
