using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using ClickHouse.Driver.ADO.Parameters;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;
using Microsoft.Extensions.VectorData.ProviderServices;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// Builds ClickHouse SQL statements. Values always flow through
/// <see cref="ClickHouseParameterCollection"/> and are substituted server-side via
/// <c>{name:Type}</c> placeholders — only identifiers (table/column names) and structural
/// keywords (LIMIT, ORDER, distance functions) are interpolated into the SQL text.
/// </summary>
internal static class ClickHouseCommandBuilder
{
    internal static ClickHouseStatement CreateTable(string tableName, bool ifNotExists, CollectionModel model)
    {
        // Key column is never Nullable — it's the ORDER BY key, and ClickHouse rejects that
        // unless allow_nullable_key is enabled.
        var columns = new List<string>
        {
            $"    {Quote(model.KeyProperty.StorageName)} {ClickHouseTypeMap.MapClickHouseType(model.KeyProperty.Type)}"
        };

        foreach (var property in model.DataProperties)
        {
            columns.Add($"    {Quote(property.StorageName)} {ClickHouseTypeMap.GetColumnType(property.Type)}");

            if (property.IsIndexed && GetDataSkipIndex(property.Type) is { } indexType)
            {
                columns.Add($"    INDEX {Quote($"{property.StorageName}_idx")} {Quote(property.StorageName)} TYPE {indexType} GRANULARITY 1");
            }
        }

        foreach (var property in model.VectorProperties)
        {
            var distance = MapDistanceFunction(property.DistanceFunction ?? DistanceFunction.CosineDistance);
            var dims = property.Dimensions.ToString(CultureInfo.InvariantCulture);
            columns.Add($"    {Quote(property.StorageName)} Array(Float32)");
            columns.Add($"    INDEX {Quote($"{property.StorageName}_idx")} {Quote(property.StorageName)} TYPE vector_similarity('hnsw', '{distance}', {dims}) GRANULARITY 1");
        }

        var existsClause = ifNotExists ? "IF NOT EXISTS " : "";
        return $"""
            CREATE TABLE {existsClause}{Quote(tableName)} (
            {string.Join(",\n", columns)}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY ({Quote(model.KeyProperty.StorageName)})
            """;
    }

    internal static ClickHouseStatement DropTableIfExists(string tableName)
        => $"DROP TABLE IF EXISTS {Quote(tableName)}";

    internal static ClickHouseStatement SelectTableName(string tableName, string database)
    {
        var parameters = new ClickHouseParameterCollection();
        var dbParam = parameters.AppendValue(database, "String");
        var nameParam = parameters.AppendValue(tableName, "String");
        return new ClickHouseStatement(
            $"SELECT name FROM system.tables WHERE database = {dbParam} AND name = {nameParam}",
            parameters);
    }

    internal static ClickHouseStatement SelectTableNames(string database)
    {
        var parameters = new ClickHouseParameterCollection();
        var dbParam = parameters.AppendValue(database, "String");
        return new ClickHouseStatement(
            $"SELECT name FROM system.tables WHERE database = {dbParam} AND engine NOT IN ('MaterializedView', 'View')",
            parameters);
    }

    internal static ClickHouseStatement SelectSingle(string tableName, CollectionModel model, object key, bool includeVectors)
    {
        var parameters = new ClickHouseParameterCollection();
        var keyParam = parameters.AppendValue(key, model.KeyProperty.Type);
        return new ClickHouseStatement(
            $"""
            SELECT {ColumnList(model.Properties, includeVectors)}
            FROM {Quote(tableName)} FINAL
            WHERE {Quote(model.KeyProperty.StorageName)} = {keyParam}
            """,
            parameters);
    }

    internal static ClickHouseStatement SelectMany(string tableName, CollectionModel model, IEnumerable<object> keys, bool includeVectors)
    {
        var parameters = new ClickHouseParameterCollection();
        var placeholders = string.Join(", ", keys.Select(k => parameters.AppendValue(k, model.KeyProperty.Type)));
        return new ClickHouseStatement(
            $"""
            SELECT {ColumnList(model.Properties, includeVectors)}
            FROM {Quote(tableName)} FINAL
            WHERE {Quote(model.KeyProperty.StorageName)} IN ({placeholders})
            """,
            parameters);
    }

    /// <summary>
    /// Builds an INSERT statement. <paramref name="generatedEmbeddings"/> is indexed
    /// as [recordIndex][vectorPropertyIndex]; either dimension may be null when no
    /// embedding generation was required.
    /// </summary>
    internal static ClickHouseStatement Upsert(
        string tableName,
        CollectionModel model,
        IEnumerable<object> records,
        Embedding<float>?[]?[]? generatedEmbeddings)
    {
        var parameters = new ClickHouseParameterCollection();
        var rows = new List<string>();
        int rowIndex = 0;

        foreach (var record in records)
        {
            var rowEmbeddings = generatedEmbeddings is not null && rowIndex < generatedEmbeddings.Length
                ? generatedEmbeddings[rowIndex]
                : null;

            int vectorIndex = 0;
            var values = new List<string>();

            foreach (var property in model.Properties)
            {
                if (property is VectorPropertyModel)
                {
                    var generated = rowEmbeddings is not null && vectorIndex < rowEmbeddings.Length
                        ? rowEmbeddings[vectorIndex]
                        : null;
                    vectorIndex++;

                    var rawValue = generated is not null ? (object)generated : property.GetValueAsObject(record);
                    values.Add(parameters.AppendValue(NormalizeVectorValue(rawValue), "Array(Float32)"));
                }
                else
                {
                    values.Add(parameters.AppendValue(property.GetValueAsObject(record), property.Type));
                }
            }

            rows.Add($"({string.Join(", ", values)})");
            rowIndex++;
        }

        return new ClickHouseStatement(
            $"INSERT INTO {Quote(tableName)} ({ColumnList(model.Properties, includeVectors: true)}) VALUES\n{string.Join(",\n", rows)}",
            parameters);
    }

    internal static ClickHouseStatement DeleteSingle(string tableName, KeyPropertyModel keyProperty, object key)
    {
        var parameters = new ClickHouseParameterCollection();
        var keyParam = parameters.AppendValue(key, keyProperty.Type);
        return new ClickHouseStatement(
            $"DELETE FROM {Quote(tableName)} WHERE {Quote(keyProperty.StorageName)} = {keyParam}",
            parameters);
    }

    internal static ClickHouseStatement DeleteMany(string tableName, KeyPropertyModel keyProperty, IEnumerable<object> keys)
    {
        var parameters = new ClickHouseParameterCollection();
        var placeholders = string.Join(", ", keys.Select(k => parameters.AppendValue(k, keyProperty.Type)));
        return new ClickHouseStatement(
            $"DELETE FROM {Quote(tableName)} WHERE {Quote(keyProperty.StorageName)} IN ({placeholders})",
            parameters);
    }

    internal static ClickHouseStatement SelectVector<TRecord>(
        string tableName,
        VectorPropertyModel vectorProperty,
        CollectionModel model,
        int top,
        VectorSearchOptions<TRecord> options,
        float[] vector)
    {
        var parameters = new ClickHouseParameterCollection();
        var vectorParam = parameters.AppendValue(vector, "Array(Float32)");
        var distanceFunc = MapDistanceFunction(vectorProperty.DistanceFunction ?? DistanceFunction.CosineDistance);
        var where = options.Filter is not null ? BuildWhere(options.Filter, model, parameters) : "";
        var offset = options.Skip > 0 ? $" OFFSET {options.Skip.ToString(CultureInfo.InvariantCulture)}" : "";

        return new ClickHouseStatement(
            $"""
            SELECT {ColumnList(model.Properties, options.IncludeVectors)},
                   {distanceFunc}({Quote(vectorProperty.StorageName)}, {vectorParam}) AS _score
            FROM {Quote(tableName)} FINAL
            {where}
            ORDER BY _score ASC
            LIMIT {top.ToString(CultureInfo.InvariantCulture)}{offset}
            """,
            parameters);
    }

    internal static ClickHouseStatement SelectWhere<TRecord>(
        Expression<Func<TRecord, bool>> filter,
        int top,
        FilteredRecordRetrievalOptions<TRecord> options,
        string tableName,
        CollectionModel model)
    {
        var parameters = new ClickHouseParameterCollection();
        var where = filter is not null ? BuildWhere(filter, model, parameters) : "";

        var orderByValues = options.OrderBy?.Invoke(new()).Values;
        var orderBy = orderByValues is { Count: > 0 }
            ? "ORDER BY " + string.Join(", ", orderByValues.Select(s =>
                $"{Quote(model.GetDataOrKeyProperty(s.PropertySelector).StorageName)} {(s.Ascending ? "ASC" : "DESC")}"))
            : "";

        var offset = options.Skip > 0 ? $" OFFSET {options.Skip.ToString(CultureInfo.InvariantCulture)}" : "";

        return new ClickHouseStatement(
            $"""
            SELECT {ColumnList(model.Properties, options.IncludeVectors)}
            FROM {Quote(tableName)} FINAL
            {where}
            {orderBy}
            LIMIT {top.ToString(CultureInfo.InvariantCulture)}{offset}
            """,
            parameters);
    }

    // --- Helpers ---

    private static string Quote(string identifier)
        => $"`{identifier.Replace("`", "``", StringComparison.Ordinal)}`";

    private static string ColumnList(IEnumerable<PropertyModel> properties, bool includeVectors)
        => string.Join(", ", properties
            .Where(p => includeVectors || p is not VectorPropertyModel)
            .Select(p => Quote(p.StorageName)));

    private static string BuildWhere<TRecord>(Expression<Func<TRecord, bool>> filter, CollectionModel model, ClickHouseParameterCollection parameters)
    {
        var sb = new StringBuilder();
        new ClickHouseFilterTranslator(sb, parameters).Translate(filter, model, appendWhere: true);
        return sb.ToString();
    }

    /// <summary>
    /// Unwraps vector property values — which may be <see cref="ReadOnlyMemory{Single}"/>,
    /// <see cref="Embedding{T}"/>, or <c>float[]</c> — into a plain array the driver can
    /// serialize as <c>Array(Float32)</c>.
    /// </summary>
    private static float[]? NormalizeVectorValue(object? value) => value switch
    {
        null => null,
        ReadOnlyMemory<float> rom => rom.ToArray(),
        Embedding<float> emb => emb.Vector.ToArray(),
        float[] f => f,
        _ => throw new NotSupportedException($"Unsupported vector value type: {value.GetType().Name}")
    };

    /// <summary>
    /// Picks a ClickHouse data-skipping index type for a data property marked <c>IsIndexed</c>.
    /// <c>minmax</c> accelerates range queries on numerics and date/time; <c>bloom_filter</c>
    /// accelerates equality/<c>has()</c> on strings, UUIDs, and array columns. <c>bool</c> is
    /// skipped — indexes over two distinct values never help.
    /// </summary>
    private static string? GetDataSkipIndex(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;

        if (ClickHouseTypeMap.TryGetCollectionElementType(t, out _, out _))
        {
            return "bloom_filter(0.01)";
        }

        if (t == typeof(bool))
        {
            return null;
        }

        if (t == typeof(int) || t == typeof(long) || t == typeof(float) || t == typeof(double)
            || t == typeof(DateTime) || t == typeof(DateTimeOffset))
        {
            return "minmax";
        }

        return "bloom_filter(0.01)";
    }

    internal static string MapDistanceFunction(string name) => name switch
    {
        DistanceFunction.CosineDistance => "cosineDistance",
        DistanceFunction.EuclideanDistance => "L2Distance",
        _ => throw new NotSupportedException($"Distance function '{name}' is not supported by the ClickHouse connector. Supported: CosineDistance, EuclideanDistance.")
    };
}
