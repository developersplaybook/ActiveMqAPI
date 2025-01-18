using Shared.Models;
using System;
using System.Threading.Tasks;

namespace Server.Interfaces
{
    public interface IServerMessageHub
    {
        Task CheckForNewClientMessage();
        Task HandleMessageFromClient(ClientQueueEntity queuePackage);
        Task SendMessageToClient(object message, Guid correlationId);
    }
}
