using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using JasperFx;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Wolverine;
using Wolverine.Postgresql;
using Wolverine.RabbitMQ;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Util;
using Xunit;

namespace WolverineRecoveryRepro;

public record Ping(Guid Id);

public static class PingHandler
{
    public static readonly ConcurrentDictionary<Guid, int> Handled = new();

    public static void Handle(Ping ping) => Handled.AddOrUpdate(ping.Id, 1, (_, count) => count + 1);
}

public sealed class Containers : IAsyncLifetime
{
    public PostgreSqlContainer Postgres { get; } = new PostgreSqlBuilder("postgres:17-alpine").Build();
    public RabbitMqContainer Rabbit { get; } = new RabbitMqBuilder("rabbitmq:4.2.3-management-alpine").Build();

    public async ValueTask InitializeAsync() => await Task.WhenAll(Postgres.StartAsync(), Rabbit.StartAsync());

    public async ValueTask DisposeAsync()
    {
        await Postgres.DisposeAsync();
        await Rabbit.DisposeAsync();
    }
}

/// <summary>
/// A dormant inbox row (owner_id = 0) for an inline endpoint is recovered by the durability agent through
/// ListeningAgent.EnqueueDirectlyAsync, which wraps the endpoint's listener in a RetryOnInlineChannelCallback.
/// With ListenerCount > 1 that listener is a ParallelListener, whose CompleteAsync forwards to envelope.Listener --
/// the RetryOnInlineChannelCallback itself -- so completing the envelope recurses forever.
/// </summary>
public sealed class InboxRecoveryTests(Containers containers) : IClassFixture<Containers>
{
    // Mark-as-handled statements per second on the inbox once recovery should be done; idle is ~0.
    private const double RunawayInboxScansPerSecond = 50;

    [Fact]
    public Task recovery_into_a_single_inline_listener_completes() => AssertRecoveryCompletesAsync(listenerCount: 1);

    [Fact]
    public Task recovery_into_parallel_inline_listeners_completes() => AssertRecoveryCompletesAsync(listenerCount: 2);

    [Fact(Explicit = true)]
    public async Task stopping_the_host_after_recovery_into_parallel_inline_listeners()
    {
        // Once the message store is disposed, MarkIncomingEnvelopeAsHandledAsync returns synchronously and the
        // recursion runs on one stack: the test host dies with "Stack overflow." instead of failing this test.
        var (host, _, ping, _) = await StartAndSeedAsync(listenerCount: 2);
        await WaitUntilAsync(() => PingHandler.Handled.ContainsKey(ping.Id), TimeSpan.FromSeconds(30));

        await host.StopAsync(TestContext.Current.CancellationToken);
        host.Dispose();
    }

    /// <summary>
    /// CloudEventsMapper gives an envelope a fresh random id whenever the CloudEvent id is not a Guid, also when it
    /// re-reads an envelope that already has one. A recovered inbox row is then marked as handled under the new id,
    /// so the row itself stays Incoming and is recovered and handled again after every restart.
    /// </summary>
    [Fact]
    public async Task recovered_cloud_event_with_a_non_guid_id_is_marked_as_handled()
    {
        var (host, _, ping, envelopeId) = await StartAndSeedAsync(listenerCount: 1, asCloudEvent: true);

        var handled = await WaitUntilAsync(() => PingHandler.Handled.ContainsKey(ping.Id), TimeSpan.FromSeconds(30));
        Assert.True(handled, "The durability agent never recovered the dormant inbox row");

        var runtime = host.Services.GetRequiredService<IWolverineRuntime>();
        string? status = null;
        await WaitUntilAsync(() =>
        {
            status = InboxStatusAsync(runtime, envelopeId).GetAwaiter().GetResult();
            return status != "Incoming";
        }, TimeSpan.FromSeconds(10));

        await host.StopAsync(TestContext.Current.CancellationToken);
        host.Dispose();

        Assert.True(status != "Incoming",
            $"Inbox row {envelopeId} is still Incoming after it was handled, so every restart recovers it again");
    }

    private async Task AssertRecoveryCompletesAsync(int listenerCount)
    {
        var (host, logs, ping, _) = await StartAndSeedAsync(listenerCount);

        var handled = await WaitUntilAsync(() => PingHandler.Handled.ContainsKey(ping.Id), TimeSpan.FromSeconds(30));
        Assert.True(handled, "The durability agent never recovered the dormant inbox row");

        var completed = await WaitUntilAsync(() => logs.Contains("Successfully recovered 1 messages"), TimeSpan.FromSeconds(10));
        var scansPerSecond = await MeasureInboxScansPerSecondAsync(host.Services.GetRequiredService<IWolverineRuntime>());

        // A graceful stop of a recursing host overflows the stack (see the Explicit test), so leave that one running.
        if (completed && scansPerSecond < RunawayInboxScansPerSecond)
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
            host.Dispose();
        }

        Assert.True(completed,
            $"Recovery never returned from EnqueueDirectlyAsync; the inbox is hit {scansPerSecond:F0} times/s");
        Assert.True(scansPerSecond < RunawayInboxScansPerSecond,
            $"Runaway mark-as-handled loop: the inbox is hit {scansPerSecond:F0} times/s after recovery");
        Assert.Equal(1, PingHandler.Handled[ping.Id]);
    }

    private async Task<(IHost Host, LogCapture Logs, Ping Ping, Guid EnvelopeId)> StartAndSeedAsync(
        int listenerCount, bool asCloudEvent = false)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var queue = $"repro-{listenerCount}-{suffix}";
        var schema = $"repro_{listenerCount}_{suffix}";
        var logs = new LogCapture();

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders().AddProvider(logs).SetMinimumLevel(LogLevel.Information);
        builder.UseWolverine(opts =>
        {
            opts.Durability.Mode = DurabilityMode.Solo;
            opts.AutoBuildMessageStorageOnStartup = AutoCreate.CreateOrUpdate;
            opts.PersistMessagesWithPostgresql(containers.Postgres.GetConnectionString(), schema);
            opts.UseRabbitMq(new Uri(containers.Rabbit.GetConnectionString())).AutoProvision();

            // ProcessInline + ListenerCount(n > 1) is what Azure Service Bus RequireSessions(n) + ProcessInline() builds.
            var listener = opts.ListenToRabbitQueue(queue).ListenerCount(listenerCount).ProcessInline();
            if (asCloudEvent) listener.InteropWithCloudEvents(new JsonSerializerOptions());

            opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(PingHandler));
        });

        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        // A message a dead node left in the inbox: Incoming, owned by nobody.
        var runtime = host.Services.GetRequiredService<IWolverineRuntime>();
        var ping = new Ping(Guid.NewGuid());
        var envelope = asCloudEvent
            ? new Envelope
            {
                // Stored as received: a CloudEvent whose id (here an Auth0 log id) is not a Guid.
                Id = Guid.NewGuid(),
                MessageType = typeof(Ping).ToMessageTypeName(),
                ContentType = "application/json",
                Data = JsonSerializer.SerializeToUtf8Bytes(new
                {
                    id = "90020260911171709093778000000000000001223372037067692204",
                    source = "/repro",
                    type = typeof(Ping).ToMessageTypeName(),
                    specversion = "1.0",
                    datacontenttype = "application/json",
                    data = ping
                })
            }
            : new Envelope(ping)
            {
                Serializer = runtime.Options.DefaultSerializer,
                ContentType = runtime.Options.DefaultSerializer.ContentType
            };
        envelope.Destination = new Uri($"rabbitmq://queue/{queue}");
        envelope.Status = EnvelopeStatus.Incoming;
        envelope.OwnerId = TransportConstants.AnyNode;
        await runtime.Storage.Inbox.StoreIncomingAsync(envelope);

        return (host, logs, ping, envelope.Id);
    }

    private static string SchemaOf(IWolverineRuntime runtime) => runtime.Storage switch
    {
        Wolverine.RDBMS.IMessageDatabase database => database.SchemaName,
        _ => throw new InvalidOperationException("Expected a relational message store")
    };

    private async Task<string?> InboxStatusAsync(IWolverineRuntime runtime, Guid envelopeId)
    {
        await using var conn = new NpgsqlConnection(containers.Postgres.GetConnectionString());
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"select status from {SchemaOf(runtime)}.wolverine_incoming_envelopes where id = @id", conn);
        cmd.Parameters.AddWithValue("id", envelopeId);
        return (string?)await cmd.ExecuteScalarAsync();
    }

    private async Task<double> MeasureInboxScansPerSecondAsync(IWolverineRuntime runtime)
    {
        var schema = SchemaOf(runtime);

        async Task<long> ScansAsync()
        {
            await using var conn = new NpgsqlConnection(containers.Postgres.GetConnectionString());
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "select coalesce(sum(seq_scan + coalesce(idx_scan, 0)), 0)::bigint from pg_stat_user_tables " +
                "where schemaname = @schema and relname like 'wolverine_incoming_envelopes%'", conn);
            cmd.Parameters.AddWithValue("schema", schema);
            return (long)(await cmd.ExecuteScalarAsync())!;
        }

        // Postgres flushes table statistics about once a second, so measure over a few.
        var before = await ScansAsync();
        var watch = Stopwatch.StartNew();
        await Task.Delay(TimeSpan.FromSeconds(4), TestContext.Current.CancellationToken);
        var after = await ScansAsync();
        return (after - before) / watch.Elapsed.TotalSeconds;
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        return condition();
    }
}

public sealed class LogCapture : ILoggerProvider
{
    private readonly ConcurrentQueue<string> _messages = new();

    public bool Contains(string text) => _messages.Any(m => m.Contains(text, StringComparison.Ordinal));

    public ILogger CreateLogger(string categoryName) => new Logger(_messages);

    public void Dispose()
    {
    }

    private sealed class Logger(ConcurrentQueue<string> messages) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel)) messages.Enqueue(formatter(state, exception));
        }
    }
}
