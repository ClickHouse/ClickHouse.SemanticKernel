using ClickHouse.ConformanceTests.Support;
using ClickHouse.Driver.ADO;
using ClickHouse.SemanticKernel;
using Xunit;

namespace ClickHouse.ConformanceTests;

/// <summary>
/// Verifies end-to-end that the connector tags its ClickHouse HTTP requests with the
/// <c>lib</c> User-Agent token, by reading it back from <c>system.query_log</c>.
/// </summary>
public class ClickHouseUserAgentTests : IClassFixture<ClickHouseFixture>
{
    public ClickHouseUserAgentTests(ClickHouseFixture fixture)
    {
        // The fixture starts (reference-counts) the shared ClickHouse container.
        _ = fixture;
    }

    [Fact]
    public async Task Queries_are_tagged_with_lib_in_user_agent()
    {
        var connectionString = ClickHouseTestStore.Instance.ConnectionString;

        // Issue a query through the library's tagged client.
        using (var store = new ClickHouseVectorStore(connectionString))
        {
            await foreach (var _ in store.ListCollectionNamesAsync())
            {
            }
        }

        // Use a separate, untagged connection for verification so the lib tag we assert
        // on can only have come from the connector's own client.
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        using (var flush = connection.CreateCommand())
        {
            flush.CommandText = "SYSTEM FLUSH LOGS";
            await flush.ExecuteNonQueryAsync();
        }

        using var query = connection.CreateCommand();
        query.CommandText =
            "SELECT count() FROM system.query_log " +
            "WHERE http_user_agent LIKE '%lib:ClickHouse.SemanticKernel%' " +
            "AND event_time > now() - INTERVAL 5 MINUTE";

        var count = Convert.ToInt64(await query.ExecuteScalarAsync());
        Assert.True(count > 0,
            "Expected at least one query tagged with lib:ClickHouse.SemanticKernel in system.query_log.");
    }
}
