using VectorData.ConformanceTests.Support;

namespace ClickHouse.ConformanceTests.Support;

public class ClickHouseFixture : VectorStoreFixture
{
    public override TestStore TestStore => ClickHouseTestStore.Instance;
}

public class ClickHouseSimpleModelFixture : SimpleModelFixture<string>
{
    public override TestStore TestStore => ClickHouseTestStore.Instance;
}

public class ClickHouseDynamicModelFixture : DynamicDataModelFixture<string>
{
    public override TestStore TestStore => ClickHouseTestStore.Instance;
}
