using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO.Readers;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;
using Microsoft.Extensions.VectorData.ProviderServices;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// An implementation of <see cref="VectorStoreCollection{TKey, TRecord}"/> backed by a ClickHouse database.
/// </summary>
#pragma warning disable CA1711 // Identifiers should not have incorrect suffix (Collection)
public class ClickHouseCollection<TKey, TRecord>
#pragma warning restore CA1711
    : VectorStoreCollection<TKey, TRecord>
    where TKey : notnull
    where TRecord : class
{
    private readonly VectorStoreCollectionMetadata _collectionMetadata;

    private static readonly VectorSearchOptions<TRecord> s_defaultVectorSearchOptions = new();

    private readonly ClickHouseClient _client;
    private readonly bool _ownsClient;
    private readonly CollectionModel _model;
    private readonly ClickHouseMapper<TRecord> _mapper;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseCollection{TKey, TRecord}"/> class
    /// with its own <see cref="ClickHouseClient"/> derived from the connection string.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public ClickHouseCollection(
        string connectionString,
        string name,
        ClickHouseCollectionOptions? options = null)
        : this(
            CreateOwnedClient(connectionString),
            ownsClient: true,
            name,
            static options => typeof(TRecord) == typeof(Dictionary<string, object?>)
                ? throw new NotSupportedException(VectorDataStrings.NonDynamicCollectionWithDictionaryNotSupported(typeof(ClickHouseDynamicCollection)))
                : new ClickHouseModelBuilder().Build(typeof(TRecord), options.Definition, options.EmbeddingGenerator),
            options)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseCollection{TKey, TRecord}"/> class
    /// using a user-supplied <see cref="ClickHouseClient"/> (which the collection does not own).
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public ClickHouseCollection(
        ClickHouseClient client,
        string name,
        ClickHouseCollectionOptions? options = null)
        : this(
            client,
            ownsClient: false,
            name,
            static options => typeof(TRecord) == typeof(Dictionary<string, object?>)
                ? throw new NotSupportedException(VectorDataStrings.NonDynamicCollectionWithDictionaryNotSupported(typeof(ClickHouseDynamicCollection)))
                : new ClickHouseModelBuilder().Build(typeof(TRecord), options.Definition, options.EmbeddingGenerator),
            options)
    {
    }

    internal ClickHouseCollection(
        ClickHouseClient client,
        bool ownsClient,
        string name,
        Func<ClickHouseCollectionOptions, CollectionModel> modelFactory,
        ClickHouseCollectionOptions? options)
    {
        Verify.NotNull(client);
        Verify.NotNull(name);

        if (typeof(TKey) != typeof(int) && typeof(TKey) != typeof(long) && typeof(TKey) != typeof(string) && typeof(TKey) != typeof(Guid) && typeof(TKey) != typeof(object))
        {
            throw new NotSupportedException($"Key type '{typeof(TKey).Name}' is not supported. Supported key types: int, long, string, Guid.");
        }

        options ??= ClickHouseCollectionOptions.Default;

        this._client = client;
        this._ownsClient = ownsClient;
        this.Name = name;
        this._model = modelFactory(options);
        this._mapper = new ClickHouseMapper<TRecord>(this._model);

        var database = string.IsNullOrEmpty(client.Settings.Database) ? "default" : client.Settings.Database;
        this._collectionMetadata = new()
        {
            VectorStoreSystemName = ClickHouseConstants.VectorStoreSystemName,
            VectorStoreName = database,
            CollectionName = name
        };
    }

    private static ClickHouseClient CreateOwnedClient(string connectionString)
    {
        Verify.NotNullOrWhiteSpace(connectionString);
        return new ClickHouseClient(connectionString);
    }

    /// <inheritdoc/>
    public override string Name { get; }

    /// <inheritdoc/>
    public override async Task<bool> CollectionExistsAsync(CancellationToken cancellationToken = default)
    {
        var sql = ClickHouseCommandBuilder.SelectTableName(this.Name, this._collectionMetadata.VectorStoreName!);
        using var reader = await this.ExecuteReaderAsync(sql, "CollectionExists", cancellationToken).ConfigureAwait(false);
        return await reader.ReadWithErrorHandlingAsync(this._collectionMetadata, "CollectionExists", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Task EnsureCollectionExistsAsync(CancellationToken cancellationToken = default)
        => this.ExecuteNonQueryAsync(
            ClickHouseCommandBuilder.CreateTable(this.Name, ifNotExists: true, this._model),
            "CreateCollection",
            cancellationToken);

    /// <inheritdoc/>
    public override Task EnsureCollectionDeletedAsync(CancellationToken cancellationToken = default)
        => this.ExecuteNonQueryAsync(
            ClickHouseCommandBuilder.DropTableIfExists(this.Name),
            "DeleteCollection",
            cancellationToken);

    /// <inheritdoc/>
    public override Task DeleteAsync(TKey key, CancellationToken cancellationToken = default)
    {
        Verify.NotNull(key);
        return this.ExecuteNonQueryAsync(
            ClickHouseCommandBuilder.DeleteSingle(this.Name, this._model.KeyProperty, key),
            "Delete",
            cancellationToken);
    }

    /// <inheritdoc/>
    public override Task DeleteAsync(IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
    {
        Verify.NotNull(keys);

        var keyObjects = keys.Cast<object>().ToList();
        if (keyObjects.Count == 0) return Task.CompletedTask;

        return this.ExecuteNonQueryAsync(
            ClickHouseCommandBuilder.DeleteMany(this.Name, this._model.KeyProperty, keyObjects),
            "DeleteBatch",
            cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<TRecord?> GetAsync(TKey key, RecordRetrievalOptions? options = null, CancellationToken cancellationToken = default)
    {
        Verify.NotNull(key);

        bool includeVectors = options?.IncludeVectors is true;
        // When an embedding generator is configured, the record's vector property is typed as
        // the generator *input* (e.g. a source string), not float[] — the column still stores
        // a float[], but there's nowhere on the record to hand it back. MEVD bans the combo;
        // every built-in connector (Pinecone/Redis/Mongo/Sqlite/...) does the same guard.
        if (includeVectors && this._model.EmbeddingGenerationRequired)
        {
            throw new NotSupportedException(VectorDataStrings.IncludeVectorsNotSupportedWithEmbeddingGeneration);
        }

        var sql = ClickHouseCommandBuilder.SelectSingle(this.Name, this._model, key, includeVectors);
        using var reader = await this.ExecuteReaderAsync(sql, "Get", cancellationToken).ConfigureAwait(false);
        return await reader.ReadWithErrorHandlingAsync(this._collectionMetadata, "Get", cancellationToken).ConfigureAwait(false)
            ? this._mapper.MapFromStorageToDataModel(reader, includeVectors)
            : null;
    }

    /// <inheritdoc/>
    public override async IAsyncEnumerable<TRecord> GetAsync(
        IEnumerable<TKey> keys,
        RecordRetrievalOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Verify.NotNull(keys);

        bool includeVectors = options?.IncludeVectors is true;
        // Banned by the MEVD contract when an embedding generator is configured — see the
        // single-key GetAsync overload above for the full reasoning.
        if (includeVectors && this._model.EmbeddingGenerationRequired)
        {
            throw new NotSupportedException(VectorDataStrings.IncludeVectorsNotSupportedWithEmbeddingGeneration);
        }

        var keyObjects = keys.Cast<object>().ToList();
        if (keyObjects.Count == 0)
        {
            yield break;
        }

        var sql = ClickHouseCommandBuilder.SelectMany(this.Name, this._model, keyObjects, includeVectors);
        using var reader = await this.ExecuteReaderAsync(sql, "GetBatch", cancellationToken).ConfigureAwait(false);

        while (await reader.ReadWithErrorHandlingAsync(this._collectionMetadata, "GetBatch", cancellationToken).ConfigureAwait(false))
        {
            yield return this._mapper.MapFromStorageToDataModel(reader, includeVectors);
        }
    }

    /// <inheritdoc/>
    public override async Task UpsertAsync(TRecord record, CancellationToken cancellationToken = default)
    {
        Verify.NotNull(record);

        var generatedEmbeddings = await this.GenerateEmbeddingsAsync([record], cancellationToken).ConfigureAwait(false);
        var sql = ClickHouseCommandBuilder.Upsert(this.Name, this._model, [record], generatedEmbeddings);
        await this.ExecuteNonQueryAsync(sql, "Upsert", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task UpsertAsync(IEnumerable<TRecord> records, CancellationToken cancellationToken = default)
    {
        Verify.NotNull(records);

        var recordsList = records is IReadOnlyList<TRecord> r ? r : records.ToList();
        if (recordsList.Count == 0) return;

        var generatedEmbeddings = await this.GenerateEmbeddingsAsync(recordsList, cancellationToken).ConfigureAwait(false);
        var sql = ClickHouseCommandBuilder.Upsert(this.Name, this._model, recordsList.Cast<object>(), generatedEmbeddings);
        await this.ExecuteNonQueryAsync(sql, "UpsertBatch", cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Generates embeddings for each record that needs them.
    /// Returns an array indexed by [recordIndex][vectorPropertyIndex], or null if no generation is needed.
    /// </summary>
    private async Task<Embedding<float>?[]?[]?> GenerateEmbeddingsAsync(IReadOnlyList<TRecord> records, CancellationToken cancellationToken)
    {
        Embedding<float>?[]?[]? allEmbeddings = null;
        var vectorPropertyCount = this._model.VectorProperties.Count;

        for (var vi = 0; vi < vectorPropertyCount; vi++)
        {
            var vectorProperty = this._model.VectorProperties[vi];

            if (ClickHouseModelBuilder.IsVectorPropertyTypeValidCore(vectorProperty.Type, out _))
            {
                continue;
            }

            Debug.Assert(vectorProperty.EmbeddingGenerator is not null);

            allEmbeddings ??= new Embedding<float>?[records.Count][];

            for (int ri = 0; ri < records.Count; ri++)
            {
                if (vectorProperty.TryGenerateEmbedding<TRecord, Embedding<float>>(records[ri], cancellationToken, out var task))
                {
                    allEmbeddings[ri] ??= new Embedding<float>?[vectorPropertyCount];
                    allEmbeddings[ri]![vi] = await task.ConfigureAwait(false);
                }
                else
                {
                    throw new InvalidOperationException(
                        $"The embedding generator configured on property '{vectorProperty.ModelName}' cannot produce an embedding of type '{typeof(Embedding<float>).Name}' for the given input type.");
                }
            }
        }

        return allEmbeddings;
    }

    #region Search

    /// <inheritdoc />
    public override async IAsyncEnumerable<VectorSearchResult<TRecord>> SearchAsync<TInput>(
        TInput searchValue,
        int top,
        VectorSearchOptions<TRecord>? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Verify.NotNull(searchValue);
        Verify.NotLessThan(top, 1);

        options ??= s_defaultVectorSearchOptions;
        // Banned by the MEVD contract when an embedding generator is configured — see the
        // single-key GetAsync overload for the full reasoning.
        if (options.IncludeVectors && this._model.EmbeddingGenerationRequired)
        {
            throw new NotSupportedException(VectorDataStrings.IncludeVectorsNotSupportedWithEmbeddingGeneration);
        }

        var vectorProperty = this._model.GetVectorPropertyOrSingle(options);

        float[] vector = searchValue switch
        {
            ReadOnlyMemory<float> r => r.ToArray(),
            float[] f => f,
            Embedding<float> e => e.Vector.ToArray(),

            _ when vectorProperty.EmbeddingGenerator is IEmbeddingGenerator<TInput, Embedding<float>> generator
                => (await generator.GenerateVectorAsync(searchValue, cancellationToken: cancellationToken).ConfigureAwait(false)).ToArray(),

            _ => vectorProperty.EmbeddingGenerator is null
                ? throw new NotSupportedException(VectorDataStrings.InvalidSearchInputAndNoEmbeddingGeneratorWasConfigured(searchValue.GetType(), ClickHouseModelBuilder.SupportedVectorTypes))
                : throw new InvalidOperationException(VectorDataStrings.IncompatibleEmbeddingGeneratorWasConfiguredForInputType(typeof(TInput), vectorProperty.EmbeddingGenerator.GetType()))
        };

        var sql = ClickHouseCommandBuilder.SelectVector(this.Name, vectorProperty, this._model, top, options, vector);
        using var reader = await this.ExecuteReaderAsync(sql, "VectorizedSearch", cancellationToken).ConfigureAwait(false);

        int scoreIndex = -1;
        while (await reader.ReadWithErrorHandlingAsync(this._collectionMetadata, "VectorizedSearch", cancellationToken).ConfigureAwait(false))
        {
            if (scoreIndex < 0)
            {
                scoreIndex = reader.GetOrdinal("_score");
            }

            yield return new VectorSearchResult<TRecord>(
                this._mapper.MapFromStorageToDataModel(reader, options.IncludeVectors),
                Convert.ToDouble(reader.GetValue(scoreIndex), System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    #endregion Search

    /// <inheritdoc />
    public override async IAsyncEnumerable<TRecord> GetAsync(
        Expression<Func<TRecord, bool>> filter,
        int top,
        FilteredRecordRetrievalOptions<TRecord>? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Verify.NotNull(filter);
        Verify.NotLessThan(top, 1);

        options ??= new();

        // Banned by the MEVD contract when an embedding generator is configured — see the
        // single-key GetAsync overload for the full reasoning.
        if (options.IncludeVectors && this._model.EmbeddingGenerationRequired)
        {
            throw new NotSupportedException(VectorDataStrings.IncludeVectorsNotSupportedWithEmbeddingGeneration);
        }

        var sql = ClickHouseCommandBuilder.SelectWhere(filter, top, options, this.Name, this._model);
        using var reader = await this.ExecuteReaderAsync(sql, "GetAsync", cancellationToken).ConfigureAwait(false);

        while (await reader.ReadWithErrorHandlingAsync(this._collectionMetadata, "GetAsync", cancellationToken).ConfigureAwait(false))
        {
            yield return this._mapper.MapFromStorageToDataModel(reader, options.IncludeVectors);
        }
    }

    /// <inheritdoc />
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        Verify.NotNull(serviceType);

        return
            serviceKey is not null ? null :
            serviceType == typeof(VectorStoreCollectionMetadata) ? this._collectionMetadata :
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

    // --- Execution helpers ---

    private Task<ClickHouseDataReader> ExecuteReaderAsync(ClickHouseStatement statement, string operationName, CancellationToken cancellationToken)
        => VectorStoreErrorHandler.RunOperationAsync<ClickHouseDataReader, DbException>(
            this._collectionMetadata,
            operationName,
            () => this._client.ExecuteReaderAsync(statement.Sql, statement.Parameters, cancellationToken: cancellationToken));

    private Task<int> ExecuteNonQueryAsync(ClickHouseStatement statement, string operationName, CancellationToken cancellationToken)
        => VectorStoreErrorHandler.RunOperationAsync<int, DbException>(
            this._collectionMetadata,
            operationName,
            () => this._client.ExecuteNonQueryAsync(statement.Sql, statement.Parameters, cancellationToken: cancellationToken));
}
