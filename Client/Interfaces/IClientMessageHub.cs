using System;
using System.Threading.Tasks;

namespace Client.Interfaces
{
    public interface IClientMessageHub
    {
        Task SendToServerMessageAsync(object message, Guid correlationId);
        Task<TResponse> ListenForServerMessageAsync<TResponse>(Guid correlationId);
    }
}
