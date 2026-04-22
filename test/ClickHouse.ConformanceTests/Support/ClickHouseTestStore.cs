using ClickHouse.Driver.ADO;
using ClickHouse.SemanticKernel;
using Microsoft.Extensions.VectorData;
using Testcontainers.ClickHouse;
using VectorData.ConformanceTests.Support;

namespace ClickHouse.ConformanceTests.Support;

#pragma warning disable CA1001

internal sealed class ClickHouseTestStore : TestStore
{
    public static ClickHouseTestStore Instance { get; } = new();

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    private string? _connectionString;

    public string ConnectionString => _connectionString ?? throw new InvalidOperationException("Not initialized");

    private ClickHouseTestStore() { }

    public override string DefaultDistanceFunction => DistanceFunction.CosineDistance;
    public override string DefaultIndexKind => IndexKind.Hnsw;

    protected override async Task StartAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
        DefaultVectorStore = new ClickHouseVectorStore(_connectionString);
    }

    protected override async Task StopAsync()
    {
        await _container.StopAsync();
    }

    public override async Task WaitForDataAsync<TKey, TRecord>(
        VectorStoreCollection<TKey, TRecord> collection,
        int recordCount,
        System.Linq.Expressions.Expression<Func<TRecord, bool>>? filter = null,
        int? vectorSize = null,
        object? dummyVector = null)
    {
        // vector_similarity index may need a moment to build
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        await base.WaitForDataAsync(collection, recordCount, filter, vectorSize, dummyVector);
    }
}
