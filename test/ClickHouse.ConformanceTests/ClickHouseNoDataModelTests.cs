using ClickHouse.ConformanceTests.Support;
using VectorData.ConformanceTests.CRUD;
using VectorData.ConformanceTests.Support;
using Xunit;

namespace ClickHouse.ConformanceTests;

public class ClickHouseNoDataModelTests(ClickHouseNoDataModelTests.Fixture fixture)
    : NoDataConformanceTests<string>(fixture), IClassFixture<ClickHouseNoDataModelTests.Fixture>
{
    public new class Fixture : NoDataConformanceTests<string>.Fixture
    {
        public override TestStore TestStore => ClickHouseTestStore.Instance;
    }
}
