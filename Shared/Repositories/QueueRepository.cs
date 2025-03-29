using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Shared.Models;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Shared.Repositories;

/// <summary>
/// Handles publishing and consuming messages from ActiveMQ queues.
/// </summary>
public class QueueRepository : IQueueRepository
{
    private readonly string _brokerUri = "tcp://localhost:61616";
    private readonly string _clientQueueName = "ClientQueue";
    private readonly string _serverQueueName = "ServerQueue";

    /// <summary>
    /// Gets a single message from the specified queue.
    /// </summary>
    private async Task<QueueEntity?> GetMessageFromQueueAsync(string queueName, Guid? correlationId = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        var factory = new ConnectionFactory(_brokerUri);

        using var connection = factory.CreateConnection();
        using var session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
        IDestination destination = session.GetQueue(queueName);

        string? selector = correlationId.HasValue ? $"CorrelationId = '{correlationId}'" : null;
        using var consumer = session.CreateConsumer(destination, selector);
        connection.Start();

        var effectiveTimeout = timeout ?? TimeSpan.FromSeconds(10);

        while (!cancellationToken.IsCancellationRequested)
        {
            var message = await consumer.ReceiveAsync(effectiveTimeout) as ITextMessage;
            if (message != null)
            {
                try
                {
                    var entity = JsonSerializer.Deserialize<QueueEntity>(message.Text);
                    message.Acknowledge();
                    return entity;
                }
                catch (Exception ex)
                {
                    // TODO: Log ex if needed
                    message.Acknowledge(); // avoid poisoning the queue
                    return null;
                }
            }
        }

        return null;
    }


    /// <summary>
    /// Adds a message to the specified queue.
    /// </summary>
    private async Task<int> AddQueueItemAsync<T>(string queueName, T entity)
    {
        var factory = new ConnectionFactory(_brokerUri);
        using var connection = factory.CreateConnection();
        using var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
        IDestination destination = session.GetQueue(queueName);

        using var producer = session.CreateProducer(destination);
        connection.Start();

        var json = JsonSerializer.Serialize(entity);
        var textMessage = session.CreateTextMessage(json);

        // Automatically map CorrelationId if exists
        var correlationIdProperty = typeof(T).GetProperty("CorrelationId");
        if (correlationIdProperty?.GetValue(entity) is Guid correlationGuid)
        {
            textMessage.Properties["CorrelationId"] = correlationGuid.ToString();
            textMessage.NMSCorrelationID = correlationGuid.ToString();
        }

        producer.Send(textMessage);
        await Task.CompletedTask; // keep method signature async-compatible
        return 1;
    }

    // --- High-level convenience methods ---
    public Task<QueueEntity?> GetMessageFromClientQueueAsync()
        => GetMessageFromQueueAsync(_clientQueueName, null, TimeSpan.FromSeconds(10));

    public Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId)
        => GetMessageFromQueueAsync(_serverQueueName, correlationId, TimeSpan.FromSeconds(10));

    public Task<int> AddClientQueueItemAsync(QueueEntity entity)
        => AddQueueItemAsync(_clientQueueName, entity);

    public Task<int> AddServerQueueItemAsync(QueueEntity entity)
        => AddQueueItemAsync(_serverQueueName, entity);
}
