using Microsoft.Extensions.VectorData;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// Options when creating a <see cref="ClickHouseCollection{TKey, TRecord}"/>.
/// </summary>
public sealed class ClickHouseCollectionOptions : VectorStoreCollectionOptions
{
    internal static readonly ClickHouseCollectionOptions Default = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseCollectionOptions"/> class.
    /// </summary>
    public ClickHouseCollectionOptions()
    {
    }

    internal ClickHouseCollectionOptions(ClickHouseCollectionOptions? source) : base(source)
    {
    }
}
