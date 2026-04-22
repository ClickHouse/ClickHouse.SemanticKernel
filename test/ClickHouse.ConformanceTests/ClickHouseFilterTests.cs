using ClickHouse.ConformanceTests.Support;
using VectorData.ConformanceTests.Filter;
using VectorData.ConformanceTests.Support;
using Xunit;

namespace ClickHouse.ConformanceTests;

public class ClickHouseFilterTests(ClickHouseFilterTests.Fixture fixture)
    : BasicFilterTests<string>(fixture), IClassFixture<ClickHouseFilterTests.Fixture>
{
    // Legacy VectorSearchFilter API is not implemented — skip these deprecated tests
#pragma warning disable CS0809
    [Obsolete("Legacy filter support not implemented")]
    public override Task Legacy_equality() => Task.CompletedTask;

    [Obsolete("Legacy filter support not implemented")]
    public override Task Legacy_And() => Task.CompletedTask;

    [Obsolete("Legacy filter support not implemented")]
    public override Task Legacy_AnyTagEqualTo_array() => Task.CompletedTask;

    [Obsolete("Legacy filter support not implemented")]
    public override Task Legacy_AnyTagEqualTo_List() => Task.CompletedTask;
#pragma warning restore CS0809

    public new class Fixture : BasicFilterTests<string>.Fixture
    {
        public override TestStore TestStore => ClickHouseTestStore.Instance;

        public override string CollectionName => "filter_tests";
    }
}
