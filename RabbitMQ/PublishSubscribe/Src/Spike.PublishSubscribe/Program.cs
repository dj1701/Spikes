using System;
using RabbitMQ.Client;

namespace Spike.PublishSubscribe
{
    public class RabbitMQ
    {
        private readonly ConnectionFactory _factory = new ConnectionFactory();
        private const string _q1 = "q1";
        private const string _q2 = "q2";
        private const string _exchange = "Exchange";
        private const string _routingKey = "green";
        
        public RabbitMQ()
        {
            //_factory.Uri = "amqp://user:pass@hostName:port/vhost";
            _factory.Uri = "amqp://localhost";
            _factory.Protocol = Protocols.DefaultProtocol;
            _factory.Port = AmqpTcpEndpoint.UseDefaultPort;
        }

        public void SendToFanOut() 
        {
            using (IConnection conn = _factory.CreateConnection())
            using (IModel channel = conn.CreateModel())
            {
                channel.QueueDeclare("TestQueue", true,false,false,null);
                channel.ExchangeDeclare("TestExchange", "fanout");
                channel.QueueBind("TestQueue", "TestExchange", "");

                const string message = "Test Message";
                var encodedMessage = System.Text.Encoding.Unicode.GetBytes(message);
                channel.BasicPublish("TestExchange", "", null, encodedMessage);
            }
        }

        public string ReceiveFromFanOut()
        {
            string message;

            using (IConnection conn = _factory.CreateConnection())
            using (IModel channel = conn.CreateModel())
            {
                //channel.QueueDeclare("TestQueue", true, false, false, null);
                //channel.ExchangeDeclare("TestExchange", "fanout");
                //channel.QueueBind("TestQueue", "TestExchange", "");

                var encodedMessage = channel.BasicGet("TestQueue", false);
                message = System.Text.Encoding.Unicode.GetString(encodedMessage.Body);
            }

            return message;
        }

        public void SendToDirect()
        {
            using (IConnection conn = _factory.CreateConnection())
            using (IModel channel = conn.CreateModel())
            {
                channel.QueueDeclare(_q1, true, false, false, null);
                channel.QueueDeclare(_q2, true, false, false, null);
                channel.ExchangeDeclare(_exchange,"direct");
                channel.QueueBind(_q1,_exchange,_routingKey);
                channel.QueueBind(_q2,_exchange,_routingKey);

                const string message = "Message sent through direct exchange type";
                var encodedMessage = System.Text.Encoding.Unicode.GetBytes(message);
                channel.BasicPublish(_exchange,_routingKey,null,encodedMessage);
            }
        }

        public string ReceiveFromDirect()
        {
            string message1, message2;

            using (IConnection conn = _factory.CreateConnection())
            using (IModel channel = conn.CreateModel())
            {
                //channel.QueueDeclare(_q1, true, false, false, null);
                //channel.QueueDeclare(_q2, true, false, false, null);
                //channel.ExchangeDeclare(_exchange, "direct");
                //channel.QueueBind(_q1, _exchange, _routingKey);
                //channel.QueueBind(_q2, _exchange, _routingKey);

                var encodedMessage1 = channel.BasicGet(_q1, false);
                var encodedMessage2 = channel.BasicGet(_q2, false);

                message1 = System.Text.Encoding.Unicode.GetString(encodedMessage1.Body);
                message2 = System.Text.Encoding.Unicode.GetString(encodedMessage2.Body);

            }

            return message1 + message2;
        }
    }
    
    class Program
    {
        static void Main(string[] args)
        {
            var rabbitMq = new RabbitMQ();
            rabbitMq.SendToFanOut();
            var receiveFromFanOut = rabbitMq.ReceiveFromFanOut();
            Console.Write("Message received from Fan Out Exchange: {0}\n\n", receiveFromFanOut);
            
            rabbitMq.SendToDirect();
            var receiveFromDirect = rabbitMq.ReceiveFromDirect();
            Console.Write("Message received from Direct Exchange: {0}\n\n", receiveFromDirect);
            Console.WriteLine("Press any key to finish");
            Console.Read();
        }
    }
}
