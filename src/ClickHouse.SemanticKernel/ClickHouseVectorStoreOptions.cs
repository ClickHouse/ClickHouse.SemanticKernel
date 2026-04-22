using Microsoft.Extensions.AI;

namespace ClickHouse.SemanticKernel;

/// <summary>
/// Options for creating a <see cref="ClickHouseVectorStore"/>.
/// </summary>
public sealed class ClickHouseVectorStoreOptions
{
    internal static readonly ClickHouseVectorStoreOptions Defaults = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseVectorStoreOptions"/> class.
    /// </summary>
    public ClickHouseVectorStoreOptions()
    {
    }

    internal ClickHouseVectorStoreOptions(ClickHouseVectorStoreOptions? source)
    {
        this.EmbeddingGenerator = source?.EmbeddingGenerator;
    }

    /// <summary>
    /// Gets or sets the default embedding generator to use when generating vectors embeddings with this vector store.
    /// </summary>
    public IEmbeddingGenerator? EmbeddingGenerator { get; set; }
}
