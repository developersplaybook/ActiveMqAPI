using Apache.NMS;
using Apache.NMS.AMQP;
using Client.Interfaces;
using Shared.Helpers;
using Shared.Models;
using System;
using System.Threading.Tasks;

namespace Client.Hub
{
    public class ClientMessageHub : IClientMessageHub
    {
        private readonly string _brokerUri = "amqp://localhost:8161"; // ActiveMQ AMQP URI
        private readonly string _clientQueueName = "client-queue"; // Client's queue name
        private readonly string _serverQueueName = "server-queue"; // Server's queue name

        public async Task SendToServerMessage(object message, Guid correlationId)
        {
            var result = Helpers.ConvertObjectToJson(message);
            var entity = new ClientQueueEntity
            {
                CorrelationId = correlationId,
                Content = result.Item1,
                TypeName = result.Item2.ToString(),
                Created = DateTime.Now,
                StatusDate = DateTime.Now,
                QueueStatus = QueueStatus.New
            };

            // Send the message to the server queue using ActiveMQ (AMQP)
            await SendMessageToActiveMQ(_serverQueueName, entity);
        }

        public async Task<TResponse> ReceiveFromServerMessage<TResponse>(Guid correlationId)
        {
            var response = await ReceiveServerMessage(correlationId);
            return (TResponse)Helpers.ConvertJsonToObject(response.Content, Helpers.GetType(response.TypeName));
        }

        private async Task<ServerQueueEntity> ReceiveServerMessage(Guid correlationId)
        {
            // Receive the response from ActiveMQ (from client queue)
            var response = await ReceiveMessageFromActiveMQ(_clientQueueName, correlationId);
            while (response == null)
            {
                await Task.Delay(100);
                response = await ReceiveMessageFromActiveMQ(_clientQueueName, correlationId);
            }

            return response;
        }

        private async Task SendMessageToActiveMQ(string queueName, ClientQueueEntity entity)
        {
            // Set up the connection to ActiveMQ
            var factory = new NmsConnectionFactory(_brokerUri);
            var connection = factory.CreateConnection();
            var session = connection.CreateSession();

            try
            {
                // Set up the destination queue
                IDestination destination = session.GetQueue(queueName);
                using (IMessageProducer producer = session.CreateProducer(destination))
                {
                    // Create and send the message
                    var textMessage = session.CreateTextMessage(Newtonsoft.Json.JsonConvert.SerializeObject(entity));
                    textMessage.Properties["CorrelationId"] = entity.CorrelationId.ToString();
                    producer.Send(textMessage);
                }
            }
            finally
            {
                // Ensure the connection and session are properly closed
                session.Close();
                connection.Close();
            }
        }

        private async Task<ServerQueueEntity> ReceiveMessageFromActiveMQ(string queueName, Guid correlationId)
        {
            // Set up the connection to ActiveMQ
            var factory = new NmsConnectionFactory(_brokerUri);
            var connection = factory.CreateConnection();
            var session = connection.CreateSession();

            try
            {
                // Set up the destination queue
                IDestination destination = session.GetQueue(queueName);
                using (IMessageConsumer consumer = session.CreateConsumer(destination))
                {
                    // Start the connection and receive the message
                    connection.Start();
                    IMessage message = consumer.Receive(); // Block until a message is received

                    if (message is ITextMessage textMessage && Guid.TryParse(textMessage.Properties["CorrelationId"].ToString(), out Guid receivedCorrelationId) && receivedCorrelationId == correlationId)
                    {
                        return Newtonsoft.Json.JsonConvert.DeserializeObject<ServerQueueEntity>(textMessage.Text);
                    }

                    return null;
                }
            }
            finally
            {
                // Ensure the connection and session are properly closed
                session.Close();
                connection.Close();
            }
        }
    }
}
