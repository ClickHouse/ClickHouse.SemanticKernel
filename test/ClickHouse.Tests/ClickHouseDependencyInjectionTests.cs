using ClickHouse.SemanticKernel;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.VectorData;
using Xunit;

namespace ClickHouse.Tests;

public class ClickHouseDependencyInjectionTests
{
    private const string ConnectionString = "Host=localhost;Port=8123;Database=default";

    [Fact]
    public void AddVectorStore_Registers_VectorStore()
    {
        var services = new ServiceCollection();
        services.AddClickHouseVectorStore(_ => ConnectionString);
        var provider = services.BuildServiceProvider();

        Assert.IsType<ClickHouseVectorStore>(provider.GetService<VectorStore>());
    }

    [Fact]
    public void AddCollection_Registers_Collection()
    {
        var services = new ServiceCollection();
        services.AddClickHouseCollection<string, TestRecord>("test", _ => ConnectionString);
        var provider = services.BuildServiceProvider();

        Assert.IsType<ClickHouseCollection<string, TestRecord>>(provider.GetService<VectorStoreCollection<string, TestRecord>>());
    }

    [Fact]
    public void AddCollection_Registers_IVectorSearchable()
    {
        var services = new ServiceCollection();
        services.AddClickHouseCollection<string, TestRecord>("test", _ => ConnectionString);
        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<IVectorSearchable<TestRecord>>());
    }

    [Fact]
    public void AddVectorStore_WithStaticConnectionString_Works()
    {
        var services = new ServiceCollection();
        services.AddClickHouseCollection<string, TestRecord>("test", ConnectionString);
        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<VectorStoreCollection<string, TestRecord>>());
    }

    [Fact]
    public void AddVectorStore_NullServices_Throws()
    {
        IServiceCollection services = null!;
        Assert.Throws<ArgumentNullException>(() => services.AddClickHouseVectorStore(_ => ConnectionString));
    }

    [Fact]
    public void AddCollection_NullName_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() =>
            services.AddClickHouseCollection<string, TestRecord>(null!, _ => ConnectionString));
    }

    private sealed class TestRecord
    {
        [VectorStoreKey]
        public string Key { get; set; } = string.Empty;

        [VectorStoreData]
        public string? Text { get; set; }

        [VectorStoreVector(3, DistanceFunction = DistanceFunction.CosineDistance)]
        public ReadOnlyMemory<float> Embedding { get; set; }
    }
}
