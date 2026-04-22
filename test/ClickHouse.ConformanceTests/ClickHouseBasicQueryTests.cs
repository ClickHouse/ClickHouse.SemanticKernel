using ClickHouse.ConformanceTests.Support;
using VectorData.ConformanceTests.Filter;
using VectorData.ConformanceTests.Support;
using Xunit;

namespace ClickHouse.ConformanceTests;

public class ClickHouseBasicQueryTests(ClickHouseBasicQueryTests.Fixture fixture)
    : BasicQueryTests<string>(fixture), IClassFixture<ClickHouseBasicQueryTests.Fixture>
{
    public new class Fixture : BasicQueryTests<string>.QueryFixture
    {
        public override TestStore TestStore => ClickHouseTestStore.Instance;

        public override string CollectionName => "query_tests";
    }
}
