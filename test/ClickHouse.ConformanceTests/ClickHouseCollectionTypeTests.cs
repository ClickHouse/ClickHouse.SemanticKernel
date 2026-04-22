using ClickHouse.ConformanceTests.Support;
using ClickHouse.SemanticKernel;
using Microsoft.Extensions.VectorData;
using Xunit;

namespace ClickHouse.ConformanceTests;

/// <summary>
/// Exercises the generalized <c>T[]</c> / <c>List&lt;T&gt;</c> column handling in the mapper,
/// command builder and model builder across all supported element types.
/// </summary>
public class ClickHouseCollectionTypeTests : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        await ClickHouseTestStore.Instance.ReferenceCountingStartAsync();
    }

    public async Task DisposeAsync()
    {
        await ClickHouseTestStore.Instance.ReferenceCountingStopAsync();
    }

    [Fact]
    public async Task RoundtripsAllSupportedCollectionTypes()
    {
        var store = new ClickHouseVectorStore(ClickHouseTestStore.Instance.ConnectionString);
        var collection = store.GetCollection<string, CollectionRecord>("collection_types");

        await collection.EnsureCollectionDeletedAsync();
        await collection.EnsureCollectionExistsAsync();
        try
        {
            var original = new CollectionRecord
            {
                Key = "row1",
                Ints = [1, 2, 3],
                Longs = [100L, 200L],
                Floats = [1.5f, 2.5f],
                Doubles = [3.14, 2.71],
                Bools = [true, false, true],
                Strings = ["a", "b", "c"],
                Guids = [new Guid("11111111-1111-1111-1111-111111111111"), new Guid("22222222-2222-2222-2222-222222222222")],
                // TODO: re-enable once the next ClickHouse.Driver release quotes DateTime64
                // elements inside Array(...) parameters.
                // Dates = [new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc), new DateTime(2025, 6, 1, 12, 0, 0, DateTimeKind.Utc)],
                IntList = [10, 20, 30],
                LongList = [1000L, 2000L],
                StringList = ["x", "y"],
                Embedding = new float[] { 0.1f, 0.2f, 0.3f }
            };

            await collection.UpsertAsync(original);

            var loaded = await collection.GetAsync("row1");

            Assert.NotNull(loaded);
            Assert.Equal(original.Ints, loaded.Ints);
            Assert.Equal(original.Longs, loaded.Longs);
            Assert.Equal(original.Floats, loaded.Floats);
            Assert.Equal(original.Doubles, loaded.Doubles);
            Assert.Equal(original.Bools, loaded.Bools);
            Assert.Equal(original.Strings, loaded.Strings);
            Assert.Equal(original.Guids, loaded.Guids);
            // TODO: re-enable alongside the Dates field above.
            // Assert.Equal(original.Dates, loaded.Dates);
            Assert.Equal(original.IntList, loaded.IntList);
            Assert.Equal(original.LongList, loaded.LongList);
            Assert.Equal(original.StringList, loaded.StringList);
        }
        finally
        {
            await collection.EnsureCollectionDeletedAsync();
        }
    }

    public class CollectionRecord
    {
        [VectorStoreKey]
        public string Key { get; set; } = "";

        [VectorStoreData] public int[] Ints { get; set; } = [];
        [VectorStoreData] public long[] Longs { get; set; } = [];
        [VectorStoreData] public float[] Floats { get; set; } = [];
        [VectorStoreData] public double[] Doubles { get; set; } = [];
        [VectorStoreData] public bool[] Bools { get; set; } = [];
        [VectorStoreData] public string[] Strings { get; set; } = [];
        [VectorStoreData] public Guid[] Guids { get; set; } = [];
        // TODO: re-enable once the next ClickHouse.Driver release quotes DateTime64
        // elements inside Array(...) parameters.
        // [VectorStoreData] public DateTime[] Dates { get; set; } = [];

        [VectorStoreData] public List<int> IntList { get; set; } = [];
        [VectorStoreData] public List<long> LongList { get; set; } = [];
        [VectorStoreData] public List<string> StringList { get; set; } = [];

        [VectorStoreVector(Dimensions: 3)]
        public ReadOnlyMemory<float> Embedding { get; set; }
    }
}
