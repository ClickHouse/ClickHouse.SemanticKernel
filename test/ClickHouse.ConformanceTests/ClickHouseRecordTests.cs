using ClickHouse.ConformanceTests.Support;
using VectorData.ConformanceTests.CRUD;
using Xunit;

namespace ClickHouse.ConformanceTests;

public class ClickHouseRecordTests(ClickHouseSimpleModelFixture fixture)
    : RecordConformanceTests<string>(fixture), IClassFixture<ClickHouseSimpleModelFixture>
{
}
