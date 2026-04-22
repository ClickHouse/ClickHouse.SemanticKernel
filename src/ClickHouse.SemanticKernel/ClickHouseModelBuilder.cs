using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;
using Microsoft.Extensions.VectorData.ProviderServices;

namespace ClickHouse.SemanticKernel;

internal sealed class ClickHouseModelBuilder : CollectionModelBuilder
{
    internal const string SupportedVectorTypes = "ReadOnlyMemory<float>, Embedding<float>, float[]";

    private static readonly CollectionModelBuildingOptions s_modelBuildingOptions = new()
    {
        RequiresAtLeastOneVector = false,
        SupportsMultipleKeys = false,
        SupportsMultipleVectors = true,
    };

    public ClickHouseModelBuilder() : base(s_modelBuildingOptions)
    {
    }

    protected override bool IsKeyPropertyTypeValid(Type type, [NotNullWhen(false)] out string? supportedTypes)
    {
        supportedTypes = "int, long, string, Guid";

        return type == typeof(int)
            || type == typeof(long)
            || type == typeof(string)
            || type == typeof(Guid);
    }

    protected override void ValidateProperty(PropertyModel propertyModel, VectorStoreCollectionDefinition? definition)
    {
        base.ValidateProperty(propertyModel, definition);

        if (propertyModel is VectorPropertyModel vectorProperty)
        {
            switch (vectorProperty.IndexKind)
            {
                case IndexKind.Hnsw or null or "":
                    break;
                default:
                    throw new NotSupportedException(
                        $"Index kind '{vectorProperty.IndexKind}' is not supported by the ClickHouse connector. Supported index kinds: Hnsw");
            }
        }
    }

    protected override bool IsDataPropertyTypeValid(Type type, [NotNullWhen(false)] out string? supportedTypes)
    {
        supportedTypes = ClickHouseTypeMap.SupportedDataTypes;
        return ClickHouseTypeMap.IsDataTypeSupported(type);
    }

    protected override bool IsVectorPropertyTypeValid(Type type, [NotNullWhen(false)] out string? supportedTypes)
        => IsVectorPropertyTypeValidCore(type, out supportedTypes);

    internal static bool IsVectorPropertyTypeValidCore(Type type, [NotNullWhen(false)] out string? supportedTypes)
    {
        supportedTypes = SupportedVectorTypes;

        return type == typeof(ReadOnlyMemory<float>)
            || type == typeof(ReadOnlyMemory<float>?)
            || type == typeof(Embedding<float>)
            || type == typeof(float[]);
    }
}
