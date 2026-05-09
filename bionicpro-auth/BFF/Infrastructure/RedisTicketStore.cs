using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Extensions.Caching.Distributed;

namespace BFF.Infrastructure;

public sealed class RedisTicketStore(IDistributedCache cache) : ITicketStore
{
    private const string KeyPrefix = "auth-ticket:";
    private static readonly TicketSerializer Serializer = TicketSerializer.Default;

    public async Task<string> StoreAsync(AuthenticationTicket ticket)
    {
        var key = $"{KeyPrefix}{Guid.NewGuid():N}";
        await RenewAsync(key, ticket);
        return key;
    }

    public async Task RenewAsync(string key, AuthenticationTicket ticket)
    {
        var serializedTicket = Serializer.Serialize(ticket);
        var expiresAtUtc = ticket.Properties.ExpiresUtc ?? DateTimeOffset.UtcNow.AddHours(8);

        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpiration = expiresAtUtc
        };

        await cache.SetAsync(key, serializedTicket, options);
    }

    public async Task<AuthenticationTicket?> RetrieveAsync(string key)
    {
        var serializedTicket = await cache.GetAsync(key);
        if (serializedTicket is null)
        {
            return null;
        }

        return Serializer.Deserialize(serializedTicket);
    }

    public Task RemoveAsync(string key)
    {
        return cache.RemoveAsync(key);
    }
}
