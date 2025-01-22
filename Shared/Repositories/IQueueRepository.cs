using Shared.Models;
using System;
using System.Threading.Tasks;

namespace Shared.Repositories;

public interface IQueueRepository
{
    Task<QueueEntity> GetMessageFromClientQueueAsync();
    Task<QueueEntity> GetMessageFromServerQueueByCorrelationIdAsync(Guid correlationId);
    Task<int> AddClientQueueItemAsync(QueueEntity entity);
    Task<int> AddServerQueueItemAsync(QueueEntity entity);
}
