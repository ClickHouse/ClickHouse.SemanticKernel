using System.Collections.Generic;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// Tags the ClickHouse client's <c>User-Agent</c> with this connector's identity so that
/// queries it issues are attributable server-side via
/// <see cref="ClickHouseClientSettings.ApplicationInfo"/> (ClickHouse.Driver 1.3.0+).
/// </summary>
internal static class ClickHouseClientIdentity
{
    /// <summary>
    /// Value of the <c>lib</c> User-Agent tag identifying this library.
    /// </summary>
    internal const string LibraryName = "ClickHouse.SemanticKernel";

    /// <summary>
    /// Creates a <see cref="ClickHouseClient"/> from a connection string with the
    /// <c>lib</c> User-Agent tag set to <see cref="LibraryName"/>.
    /// </summary>
    internal static ClickHouseClient CreateTaggedClient(string connectionString)
        => new(new ClickHouseClientSettings(connectionString)
        {
            ApplicationInfo = new Dictionary<string, string> { ["lib"] = LibraryName },
        });
}
