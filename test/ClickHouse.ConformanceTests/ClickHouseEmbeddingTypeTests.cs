using ClickHouse.ConformanceTests.Support;
using VectorData.ConformanceTests;
using VectorData.ConformanceTests.Support;
using Xunit;

#pragma warning disable CA2000 // Dispose objects before losing scope

namespace ClickHouse.ConformanceTests;

public class ClickHouseEmbeddingTypeTests(ClickHouseEmbeddingTypeTests.Fixture fixture)
    : EmbeddingTypeTests<string>(fixture), IClassFixture<ClickHouseEmbeddingTypeTests.Fixture>
{
    public new class Fixture : EmbeddingTypeTests<string>.Fixture
    {
        public override TestStore TestStore => ClickHouseTestStore.Instance;

        public override string CollectionName => "embedding_type_tests";
    }
}
