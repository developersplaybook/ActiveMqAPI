using Shared.Models;
using System;
using System.Threading.Tasks;

namespace Shared.Interfaces;

public interface IClientQueueRepository
{
    Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId);
    Task<int> AddClientQueueItemAsync(QueueEntity entity);
}
