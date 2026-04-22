using ClickHouse.ConformanceTests.Support;
using VectorData.ConformanceTests.CRUD;
using Xunit;

namespace ClickHouse.ConformanceTests;

public class ClickHouseDynamicModelTests(ClickHouseDynamicModelFixture fixture)
    : DynamicDataModelConformanceTests<string>(fixture), IClassFixture<ClickHouseDynamicModelFixture>
{
}
