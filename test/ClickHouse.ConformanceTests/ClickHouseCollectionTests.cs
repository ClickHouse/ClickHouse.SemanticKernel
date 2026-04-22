using ClickHouse.ConformanceTests.Support;
using VectorData.ConformanceTests.Collections;
using Xunit;

namespace ClickHouse.ConformanceTests;

public class ClickHouseCollectionTests(ClickHouseFixture fixture)
    : CollectionConformanceTests<string>(fixture), IClassFixture<ClickHouseFixture>
{
    public override string CollectionName => "collection_tests";
}
