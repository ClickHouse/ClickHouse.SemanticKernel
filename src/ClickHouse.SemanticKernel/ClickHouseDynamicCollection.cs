using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// Represents a collection of vector store records in a ClickHouse database, mapped to a dynamic <c>Dictionary&lt;string, object?&gt;</c>.
/// </summary>
#pragma warning disable CA1711 // Identifiers should not have incorrect suffix
public sealed class ClickHouseDynamicCollection : ClickHouseCollection<object, Dictionary<string, object?>>
#pragma warning restore CA1711 // Identifiers should not have incorrect suffix
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseDynamicCollection"/> class
    /// with its own <see cref="ClickHouseClient"/> derived from the connection string.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public ClickHouseDynamicCollection(string connectionString, string name, ClickHouseCollectionOptions options)
        : base(
            CreateOwnedClient(connectionString),
            ownsClient: true,
            name,
            static options => new ClickHouseModelBuilder()
                .BuildDynamic(
                    options.Definition ?? throw new ArgumentException("RecordDefinition is required for dynamic collections"),
                    options.EmbeddingGenerator),
            options)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseDynamicCollection"/> class
    /// using a user-supplied <see cref="ClickHouseClient"/> (which the collection does not own).
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public ClickHouseDynamicCollection(ClickHouseClient client, string name, ClickHouseCollectionOptions options)
        : base(
            client,
            ownsClient: false,
            name,
            static options => new ClickHouseModelBuilder()
                .BuildDynamic(
                    options.Definition ?? throw new ArgumentException("RecordDefinition is required for dynamic collections"),
                    options.EmbeddingGenerator),
            options)
    {
    }

    internal ClickHouseDynamicCollection(ClickHouseClient client, bool ownsClient, string name, ClickHouseCollectionOptions options)
        : base(
            client,
            ownsClient,
            name,
            static options => new ClickHouseModelBuilder()
                .BuildDynamic(
                    options.Definition ?? throw new ArgumentException("RecordDefinition is required for dynamic collections"),
                    options.EmbeddingGenerator),
            options)
    {
    }

    private static ClickHouseClient CreateOwnedClient(string connectionString)
    {
        Verify.NotNullOrWhiteSpace(connectionString);
        return new ClickHouseClient(connectionString);
    }
}
