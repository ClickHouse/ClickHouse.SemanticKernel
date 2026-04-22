using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData.ProviderServices;

namespace ClickHouse.SemanticKernel;

internal sealed class ClickHouseMapper<TRecord>(CollectionModel model)
{
    public TRecord MapFromStorageToDataModel(DbDataReader reader, bool includeVectors)
    {
        var record = model.CreateRecord<TRecord>()!;

        PopulateValue(reader, model.KeyProperty, record);

        foreach (var property in model.DataProperties)
        {
            PopulateValue(reader, property, record);
        }

        if (includeVectors)
        {
            foreach (var property in model.VectorProperties)
            {
                try
                {
                    var ordinal = reader.GetOrdinal(property.StorageName);

                    if (!reader.IsDBNull(ordinal))
                    {
                        // Vector columns are always declared Array(Float32), which the driver
                        // surfaces as float[] via ArrayType.Read → Array.CreateInstance(framework).
                        var floatArray = (float[])reader.GetValue(ordinal);

                        property.SetValueAsObject(record, property.Type switch
                        {
                            var t when t == typeof(ReadOnlyMemory<float>) => (ReadOnlyMemory<float>)floatArray,
                            var t when t == typeof(Embedding<float>) => new Embedding<float>(floatArray),
                            var t when t == typeof(float[]) => floatArray,
                            _ => throw new NotSupportedException($"Unsupported vector type '{property.Type.Name}'.")
                        });
                    }
                }
                catch (Exception e) when (e is not InvalidOperationException and not NotSupportedException)
                {
                    throw new InvalidOperationException($"Failed to deserialize vector property '{property.ModelName}'.", e);
                }
            }
        }

        return record;
    }

    private static void PopulateValue(DbDataReader reader, PropertyModel property, object record)
    {
        try
        {
            var ordinal = reader.GetOrdinal(property.StorageName);

            if (reader.IsDBNull(ordinal))
            {
                property.SetValueAsObject(record, null);
                return;
            }

            var propertyType = Nullable.GetUnderlyingType(property.Type) ?? property.Type;

            if (ClickHouseTypeMap.TryGetCollectionElementType(propertyType, out var elementType, out var isList))
            {
                property.SetValueAsObject(record, ReadCollection(reader, ordinal, elementType, isList));
                return;
            }

            switch (propertyType)
            {
                case var t when t == typeof(int):
                    property.SetValue(record, reader.GetInt32(ordinal));
                    break;
                case var t when t == typeof(long):
                    property.SetValue(record, reader.GetInt64(ordinal));
                    break;
                case var t when t == typeof(float):
                    property.SetValue(record, reader.GetFloat(ordinal));
                    break;
                case var t when t == typeof(double):
                    property.SetValue(record, reader.GetDouble(ordinal));
                    break;
                case var t when t == typeof(string):
                    property.SetValue(record, reader.GetString(ordinal));
                    break;
                case var t when t == typeof(Guid):
                    property.SetValue(record, reader.GetGuid(ordinal));
                    break;
                case var t when t == typeof(bool):
                    property.SetValue(record, reader.GetBoolean(ordinal));
                    break;
                case var t when t == typeof(DateTime):
                    property.SetValue(record, reader.GetDateTime(ordinal));
                    break;
                case var t when t == typeof(DateTimeOffset):
                    property.SetValue(record, new DateTimeOffset(reader.GetDateTime(ordinal), TimeSpan.Zero));
                    break;

                default:
                    throw new NotSupportedException($"Unsupported type '{property.Type.Name}' for property '{property.ModelName}'.");
            }
        }
        catch (Exception ex) when (ex is not NotSupportedException)
        {
            throw new InvalidOperationException($"Failed to read property '{property.ModelName}' of type '{property.Type.Name}'.", ex);
        }
    }

    /// <summary>
    /// Reads a ClickHouse Array(T) column and boxes it as either <c>T[]</c> or <c>List&lt;T&gt;</c>
    /// at runtime. The driver returns arrays either already typed (e.g. <c>string[]</c>) or as
    /// <c>object[]</c>, depending on the element type.
    /// </summary>
    private static object ReadCollection(DbDataReader reader, int ordinal, Type elementType, bool asList)
    {
        var raw = reader.GetValue(ordinal);
        var source = raw as IEnumerable ?? Array.Empty<object>();

        var target = Array.CreateInstance(elementType, CountOf(source));
        int i = 0;
        foreach (var element in source)
        {
            target.SetValue(ConvertElement(element, elementType), i++);
        }

        if (!asList) return target;

        var listType = typeof(List<>).MakeGenericType(elementType);
        return Activator.CreateInstance(listType, target)!;
    }

    private static int CountOf(IEnumerable source) => source switch
    {
        ICollection c => c.Count,
        _ => CountEnumerable(source)
    };

    private static int CountEnumerable(IEnumerable source)
    {
        int count = 0;
        foreach (var _ in source) count++;
        return count;
    }

    private static object? ConvertElement(object? value, Type elementType)
    {
        if (value is null || value is DBNull)
        {
            return elementType == typeof(string) ? "" : null;
        }

        if (elementType.IsInstanceOfType(value))
        {
            return value;
        }

        return elementType switch
        {
            _ when elementType == typeof(string) => value.ToString() ?? "",
            _ when elementType == typeof(Guid) => value is Guid g ? g : Guid.Parse(value.ToString()!),
            _ when elementType == typeof(DateTimeOffset) => new DateTimeOffset(Convert.ToDateTime(value, CultureInfo.InvariantCulture), TimeSpan.Zero),
            _ => Convert.ChangeType(value, elementType, CultureInfo.InvariantCulture)
        };
    }
}
