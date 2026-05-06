using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Authorization;
using BFF.Infrastructure;
using Yarp.ReverseProxy.Configuration;
using Yarp.ReverseProxy.Transforms;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>() ?? ["http://localhost:3000"];
builder.Services.AddCors(options =>
{
    options.AddPolicy("frontend", policy =>
    {
        policy.WithOrigins(allowedOrigins)
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials();
    });
});
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("authenticated", policy => policy.RequireAuthenticatedUser());
});
var apiBaseUrl = (builder.Configuration["Api:BaseUrl"] ?? "https://my.api").TrimEnd('/');
builder.Services.AddReverseProxy()
    .LoadFromMemory(
        [
            new RouteConfig
            {
                RouteId = "api-route",
                ClusterId = "api-cluster",
                Match = new RouteMatch { Path = "/api/{**catch-all}" },
                AuthorizationPolicy = "authenticated"
            }
        ],
        [
            new ClusterConfig
            {
                ClusterId = "api-cluster",
                Destinations = new Dictionary<string, DestinationConfig>
                {
                    ["destination"] = new() { Address = $"{apiBaseUrl}/" }
                }
            }
        ])
    .AddTransforms(builderContext =>
    {
        builderContext.AddRequestTransform(async transformContext =>
        {
            var accessToken = await transformContext.HttpContext.GetTokenAsync("access_token");
            if (!string.IsNullOrWhiteSpace(accessToken))
            {
                transformContext.ProxyRequest.Headers.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);
            }
        });
    });
builder.Services.AddStackExchangeRedisCache(options =>
{
    options.Configuration = builder.Configuration["Redis:Configuration"];
    options.InstanceName = builder.Configuration["Redis:InstanceName"] ?? "bff:";
});
builder.Services.AddSingleton<ITicketStore, RedisTicketStore>();
builder.Services.AddOptions<CookieAuthenticationOptions>(CookieAuthenticationDefaults.AuthenticationScheme)
    .Configure<ITicketStore>((options, ticketStore) =>
    {
        options.SessionStore = ticketStore;
    });
builder.Services.AddAuthentication(options =>
{
    options.DefaultScheme = CookieAuthenticationDefaults.AuthenticationScheme;
    options.DefaultChallengeScheme = OpenIdConnectDefaults.AuthenticationScheme;
})
.AddCookie(CookieAuthenticationDefaults.AuthenticationScheme, options =>
{
    // "__Host-" cookies require Secure=true and HTTPS. In local HTTP dev, browsers reject them.
    options.Cookie.Name = builder.Environment.IsDevelopment() ? "bff" : "__Host-bff";
    options.Cookie.HttpOnly = true;
    options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
    options.Cookie.SameSite = SameSiteMode.Lax;
    options.SlidingExpiration = true;
    options.ExpireTimeSpan = TimeSpan.FromHours(8);
})
.AddOpenIdConnect(OpenIdConnectDefaults.AuthenticationScheme, options =>
{
    options.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
    var authority = builder.Configuration["OpenIdConnect:Authority"] ?? throw new InvalidOperationException("OpenIdConnect:Authority is not configured.");
    var publicAuthority = builder.Configuration["OpenIdConnect:PublicAuthority"];
    options.Authority = authority;
    options.RequireHttpsMetadata = builder.Configuration.GetValue("OpenIdConnect:RequireHttpsMetadata", !builder.Environment.IsDevelopment());
    options.ClientId = builder.Configuration["OpenIdConnect:ClientId"] ?? throw new InvalidOperationException("OpenIdConnect:ClientId is not configured.");
    options.ClientSecret = builder.Configuration["OpenIdConnect:ClientSecret"] ?? throw new InvalidOperationException("OpenIdConnect:ClientSecret is not configured.");
    options.UsePkce = true;
    options.ResponseType = "code";
    options.SaveTokens = true;
    options.Scope.Clear();

    var configuredScopes = builder.Configuration.GetSection("OpenIdConnect:Scopes").Get<string[]>();
    var scopes = configuredScopes is { Length: > 0 } ? configuredScopes : ["openid", "profile"];

    foreach (var scope in scopes)
    {
        options.Scope.Add(scope);
    }

    options.ClaimActions.MapJsonKey("role", "role");
    options.MapInboundClaims = false;
    var validIssuers = new List<string> { authority };
    if (!string.IsNullOrWhiteSpace(publicAuthority))
    {
        validIssuers.Add(publicAuthority);
    }

    options.TokenValidationParameters = new()
    {
        RoleClaimType = "role",
        NameClaimType = "given_name",
        ValidIssuers = validIssuers.Distinct(StringComparer.OrdinalIgnoreCase)
    };

    options.Events.OnRedirectToIdentityProvider = context =>
    {
        // Avoid redirecting XHR/API requests to IdP to prevent browser CORS errors.
        if (context.Request.Path.StartsWithSegments("/api") || context.Request.Path.StartsWithSegments("/bff/user"))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.HandleResponse();
            return Task.CompletedTask;
        }

        if (!string.IsNullOrWhiteSpace(publicAuthority))
        {
            context.ProtocolMessage.IssuerAddress = context.ProtocolMessage.IssuerAddress.Replace(authority, publicAuthority, StringComparison.OrdinalIgnoreCase);
        }

        return Task.CompletedTask;
    };

    options.Events.OnRedirectToIdentityProviderForSignOut = context =>
    {
        if (!string.IsNullOrWhiteSpace(publicAuthority))
        {
            context.ProtocolMessage.IssuerAddress = context.ProtocolMessage.IssuerAddress.Replace(authority, publicAuthority, StringComparison.OrdinalIgnoreCase);
        }

        return Task.CompletedTask;
    };
});


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseRouting();
app.UseCors("frontend");
app.UseAuthentication();
app.UseAuthorization();

app.MapGet("/bff/login", (HttpContext httpContext, string? returnUrl) =>
{
    var redirectUri = string.IsNullOrWhiteSpace(returnUrl) ? "/" : returnUrl;
    var properties = new AuthenticationProperties { RedirectUri = redirectUri };
    return Results.Challenge(properties, [OpenIdConnectDefaults.AuthenticationScheme]);
});

app.MapGet("/bff/logout", (string? returnUrl) =>
{
    var redirectUri = string.IsNullOrWhiteSpace(returnUrl) ? "/" : returnUrl;
    var properties = new AuthenticationProperties { RedirectUri = redirectUri };
    return Results.SignOut(properties,
        [
            CookieAuthenticationDefaults.AuthenticationScheme,
            OpenIdConnectDefaults.AuthenticationScheme
        ]);
});

app.MapGet("/bff/user", [Authorize] (ClaimsPrincipal user) =>
{
    var claims = user.Claims.Select(claim => new
    {
        type = claim.Type,
        value = claim.Value
    });

    return Results.Json(claims);
});

app.MapReverseProxy();

app.Run();
