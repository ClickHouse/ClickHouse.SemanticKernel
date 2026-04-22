using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;
using Microsoft.Extensions.VectorData.ProviderServices;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// An implementation of <see cref="VectorStore"/> backed by a ClickHouse database.
/// </summary>
public sealed class ClickHouseVectorStore : VectorStore
{
    private readonly ClickHouseClient _client;
    private readonly bool _ownsClient;
    private readonly VectorStoreMetadata _metadata;
    private readonly string _database;

    private static readonly VectorStoreCollectionDefinition s_generalPurposeDefinition = new() { Properties = [new VectorStoreKeyProperty("Key", typeof(string))] };

    private readonly IEmbeddingGenerator? _embeddingGenerator;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseVectorStore"/> class with its own
    /// <see cref="ClickHouseClient"/> derived from the connection string.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public ClickHouseVectorStore(string connectionString, ClickHouseVectorStoreOptions? options = null)
        : this(CreateOwnedClient(connectionString), ownsClient: true, options)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseVectorStore"/> class using a
    /// user-supplied <see cref="ClickHouseClient"/> (which the store does not own).
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public ClickHouseVectorStore(ClickHouseClient client, ClickHouseVectorStoreOptions? options = null)
        : this(client, ownsClient: false, options)
    {
    }

    private ClickHouseVectorStore(ClickHouseClient client, bool ownsClient, ClickHouseVectorStoreOptions? options)
    {
        Verify.NotNull(client);

        this._client = client;
        this._ownsClient = ownsClient;

        options ??= ClickHouseVectorStoreOptions.Defaults;
        this._embeddingGenerator = options.EmbeddingGenerator;

        this._database = string.IsNullOrEmpty(client.Settings.Database) ? "default" : client.Settings.Database;

        this._metadata = new()
        {
            VectorStoreSystemName = ClickHouseConstants.VectorStoreSystemName,
            VectorStoreName = this._database
        };
    }

    private static ClickHouseClient CreateOwnedClient(string connectionString)
    {
        Verify.NotNullOrWhiteSpace(connectionString);
        return new ClickHouseClient(connectionString);
    }

    /// <inheritdoc/>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public override ClickHouseCollection<TKey, TRecord> GetCollection<TKey, TRecord>(string name, VectorStoreCollectionDefinition? definition = null)
        => typeof(TRecord) == typeof(Dictionary<string, object?>)
            ? throw new ArgumentException(VectorDataStrings.GetCollectionWithDictionaryNotSupported)
            : new ClickHouseCollection<TKey, TRecord>(
                this._client,
                ownsClient: false,
                name,
                static options => new ClickHouseModelBuilder().Build(typeof(TRecord), options.Definition, options.EmbeddingGenerator),
                new ClickHouseCollectionOptions
                {
                    Definition = definition,
                    EmbeddingGenerator = this._embeddingGenerator
                });

    /// <inheritdoc />
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public override ClickHouseDynamicCollection GetDynamicCollection(string name, VectorStoreCollectionDefinition definition)
        => new ClickHouseDynamicCollection(
            this._client,
            ownsClient: false,
            name,
            new ClickHouseCollectionOptions
            {
                Definition = definition,
                EmbeddingGenerator = this._embeddingGenerator,
            });

    /// <inheritdoc/>
    public override async IAsyncEnumerable<string> ListCollectionNamesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var statement = ClickHouseCommandBuilder.SelectTableNames(this._database);
        using var reader = await VectorStoreErrorHandler.RunOperationAsync<Driver.ADO.Readers.ClickHouseDataReader, DbException>(
            this._metadata,
            operationName: "ListCollectionNames",
            () => this._client.ExecuteReaderAsync(statement.Sql, statement.Parameters, cancellationToken: cancellationToken)).ConfigureAwait(false);

        while (await reader.ReadWithErrorHandlingAsync(
            this._metadata,
            operationName: "ListCollectionNames",
            cancellationToken).ConfigureAwait(false))
        {
            yield return reader.GetString(0);
        }
    }

    /// <inheritdoc />
    public override Task<bool> CollectionExistsAsync(string name, CancellationToken cancellationToken = default)
    {
        var collection = this.GetDynamicCollection(name, s_generalPurposeDefinition);
        return collection.CollectionExistsAsync(cancellationToken);
    }

    /// <inheritdoc />
    public override Task EnsureCollectionDeletedAsync(string name, CancellationToken cancellationToken = default)
    {
        var collection = this.GetDynamicCollection(name, s_generalPurposeDefinition);
        return collection.EnsureCollectionDeletedAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        Verify.NotNull(serviceType);

        return
            serviceKey is not null ? null :
            serviceType == typeof(VectorStoreMetadata) ? this._metadata :
            serviceType == typeof(ClickHouseClient) ? this._client :
            serviceType.IsInstanceOfType(this) ? this :
            null;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && this._ownsClient)
        {
            this._client.Dispose();
        }
        base.Dispose(disposing);
    }
}
