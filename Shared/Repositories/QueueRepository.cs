using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Shared.Models;
using System;
using System.Threading.Tasks;


namespace Shared.Repositories;

public class QueueRepository : IQueueRepository
{
    private readonly string _brokerUri = "tcp://localhost:61616";
    private readonly string _clientQueueName = "ClientQueue";
    private readonly string _serverQueueName = "ServerQueue";

    public async Task<QueueEntity> GetMessageFromClientQueueAsync()
    {
        var factory = new ConnectionFactory(_brokerUri);

        using (var connection = factory.CreateConnection()) {

            using var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IDestination destination = session.GetQueue(_clientQueueName);

            using var consumer = session.CreateConsumer(destination);
            connection.Start();

            var message = await consumer.ReceiveAsync(TimeSpan.FromSeconds(10)) as ITextMessage;
            if (message != null)
            {
                return System.Text.Json.JsonSerializer.Deserialize<QueueEntity>(message.Text);
            }

            return null; // Handle appropriately if no message is found
        }
    }

    public async Task<int> AddClientQueueItemAsync(QueueEntity entity)
    {
        return await AddQueueItemAsync(_clientQueueName, entity);
    }

    public async Task<QueueEntity> GetMessageFromServerByCorrelationIdAsync(Guid correlationId)
    {
        using var connection = CreateConnection();
        using var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

        string selector = $"CorrelationId = '{correlationId}'";
        IDestination destination = session.GetQueue(_serverQueueName);

        using var consumer = session.CreateConsumer(destination, selector);
        connection.Start();

        while (true)
        {
            var message = await consumer.ReceiveAsync(TimeSpan.FromSeconds(2)) as ITextMessage;
            if (message != null)
            {
                var entity = System.Text.Json.JsonSerializer.Deserialize<QueueEntity>(message.Text);
                return entity;
            }
        }
    }

    public async Task<int> AddServerQueueItemAsync(QueueEntity entity)
    {
        return await AddQueueItemAsync(_serverQueueName, entity);
    }

    private async Task<int> AddQueueItemAsync<T>(string queueName, T entity)
    {
        using var connection = CreateConnection();
        using var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
        IDestination destination = session.GetQueue(queueName);

        using var producer = session.CreateProducer(destination);
        connection.Start();

        var json = System.Text.Json.JsonSerializer.Serialize(entity);
        var textMessage = session.CreateTextMessage(json);
        var correlationIdProperty = typeof(T).GetProperty("CorrelationId");
        if (correlationIdProperty != null)
        {
            var correlationIdValue = correlationIdProperty.GetValue(entity)?.ToString();
            if (!string.IsNullOrEmpty(correlationIdValue))
            {
                textMessage.Properties["CorrelationId"] = correlationIdValue;
            }
        }
        await Task.Run(() => producer.Send(textMessage));

        return 1; // Return 1 to indicate success (can be adjusted)
    }
    private IConnection CreateConnection()
    {
        var factory = new ConnectionFactory(_brokerUri);
        return factory.CreateConnection();
    }
}