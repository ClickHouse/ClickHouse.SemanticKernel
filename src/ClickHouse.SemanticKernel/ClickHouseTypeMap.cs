using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// Central mapping between .NET types and ClickHouse column types.
/// </summary>
internal static class ClickHouseTypeMap
{
    internal const string SupportedScalars = "string, int, long, float, double, bool, DateTime, DateTimeOffset, Guid";

    internal const string SupportedDataTypes = SupportedScalars + ", arrays and List<T> of the above (except DateTime/DateTimeOffset elements)";

    internal static bool IsScalarSupported(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        return t == typeof(int)
            || t == typeof(long)
            || t == typeof(float)
            || t == typeof(double)
            || t == typeof(bool)
            || t == typeof(string)
            || t == typeof(Guid)
            || t == typeof(DateTime)
            || t == typeof(DateTimeOffset);
    }

    internal static bool IsDataTypeSupported(Type type)
        => IsScalarSupported(type)
            || (TryGetCollectionElementType(type, out var elementType, out _)
                // Array elements cannot themselves be Nullable in ClickHouse, so reject T?[] / List<T?>.
                && Nullable.GetUnderlyingType(elementType) is null
                && IsScalarSupported(elementType)
                // TODO: re-enable once ClickHouse.Driver quotes DateTime64 elements inside
                // Array(...) parameters — until then the upsert path fails at runtime.
                && elementType != typeof(DateTime)
                && elementType != typeof(DateTimeOffset));

    internal static bool TryGetCollectionElementType(Type type, [NotNullWhen(true)] out Type? elementType, out bool isList)
    {
        if (type.IsArray && type.GetElementType() is Type array)
        {
            elementType = array;
            isList = false;
            return true;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            elementType = type.GetGenericArguments()[0];
            isList = true;
            return true;
        }

        elementType = null;
        isList = false;
        return false;
    }

    /// <summary>
    /// Maps a .NET type to its ClickHouse storage type (no Nullable wrapping — callers add that).
    /// </summary>
    internal static string MapClickHouseType(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;

        if (TryGetCollectionElementType(t, out var elementType, out _))
        {
            return $"Array({MapScalarType(elementType)})";
        }

        return MapScalarType(t);
    }

    /// <summary>
    /// Maps a .NET type to its full ClickHouse column type, including <c>Nullable(...)</c>
    /// wrapping for reference types and <see cref="Nullable{T}"/>. Array columns never get
    /// <c>Nullable(...)</c>, since ClickHouse <c>Array(T)</c> is not nullable itself.
    /// </summary>
    internal static string GetColumnType(Type type)
    {
        var chType = MapClickHouseType(type);
        bool isNullable = Nullable.GetUnderlyingType(type) is not null || !type.IsValueType;
        return isNullable && !chType.StartsWith("Array(", StringComparison.Ordinal)
            ? $"Nullable({chType})"
            : chType;
    }

    internal static string MapScalarType(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        return t switch
        {
            _ when t == typeof(int) => "Int32",
            _ when t == typeof(long) => "Int64",
            _ when t == typeof(Guid) => "UUID",
            _ when t == typeof(string) => "String",
            _ when t == typeof(bool) => "Bool",
            _ when t == typeof(DateTime) => "DateTime64(3)",
            _ when t == typeof(DateTimeOffset) => "DateTime64(3)",
            _ when t == typeof(float) => "Float32",
            _ when t == typeof(double) => "Float64",
            _ => throw new NotSupportedException($"Type {type} is not supported.")
        };
    }
}
