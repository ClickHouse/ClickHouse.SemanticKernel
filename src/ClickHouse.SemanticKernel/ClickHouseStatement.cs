using System;
using System.Globalization;
using ClickHouse.Driver.ADO.Parameters;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// A SQL statement together with its bound parameters. Values are never concatenated into
/// the SQL text — the driver passes them as <c>param_*</c> entries on the request URL and
/// the ClickHouse server substitutes them into the <c>{name:Type}</c> placeholders.
/// </summary>
/// <remarks>
/// Implicitly constructible from a bare <see cref="string"/> so parameterless DDL statements
/// (<c>CREATE TABLE</c>, <c>DROP TABLE</c>) stay terse at their call sites.
/// </remarks>
internal readonly record struct ClickHouseStatement(string Sql, ClickHouseParameterCollection? Parameters = null)
{
    public static implicit operator ClickHouseStatement(string sql) => new(sql);
}

/// <summary>
/// Extensions that build <c>{name:Type}</c> placeholders while appending a matching
/// <see cref="ClickHouseDbParameter"/> to the collection. Used by the command builder and
/// filter translator so user-supplied values never hit the SQL string directly.
/// </summary>
internal static class ClickHouseParameterExtensions
{
    /// <summary>
    /// Appends a parameter carrying <paramref name="value"/> with an explicit ClickHouse
    /// type and returns the <c>{name:Type}</c> placeholder to embed in SQL.
    /// </summary>
    public static string AppendValue(this ClickHouseParameterCollection parameters, object? value, string clickHouseType)
    {
        var name = $"p{parameters.Count.ToString(CultureInfo.InvariantCulture)}";
        parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = name,
            ClickHouseType = clickHouseType,
            Value = value ?? DBNull.Value,
        });
        return $"{{{name}:{clickHouseType}}}";
    }

    /// <summary>
    /// Appends a parameter whose ClickHouse type is inferred from the declared .NET type
    /// <paramref name="declaredType"/> (with <c>Nullable(...)</c> wrapping applied for
    /// reference types and <see cref="Nullable{T}"/>).
    /// </summary>
    public static string AppendValue(this ClickHouseParameterCollection parameters, object? value, Type declaredType)
        => parameters.AppendValue(value, ClickHouseTypeMap.GetColumnType(declaredType));
}
