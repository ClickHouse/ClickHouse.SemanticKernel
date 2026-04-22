using ClickHouse.ConformanceTests.Support;
using ClickHouse.SemanticKernel;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.VectorData;
using VectorData.ConformanceTests;
using VectorData.ConformanceTests.Support;
using Xunit;

namespace ClickHouse.ConformanceTests;

public class ClickHouseEmbeddingGenerationTests(
    ClickHouseEmbeddingGenerationTests.StringVectorFixture stringVectorFixture,
    ClickHouseEmbeddingGenerationTests.RomOfFloatVectorFixture romOfFloatVectorFixture)
    : EmbeddingGenerationTests<string>(stringVectorFixture, romOfFloatVectorFixture),
      IClassFixture<ClickHouseEmbeddingGenerationTests.StringVectorFixture>,
      IClassFixture<ClickHouseEmbeddingGenerationTests.RomOfFloatVectorFixture>
{
    public new class StringVectorFixture : EmbeddingGenerationTests<string>.StringVectorFixture
    {
        public override TestStore TestStore => ClickHouseTestStore.Instance;

        public override string CollectionName => "embedding_generation_tests";

        public override VectorStore CreateVectorStore(IEmbeddingGenerator? embeddingGenerator)
            => new ClickHouseVectorStore(
                ClickHouseTestStore.Instance.ConnectionString,
                new() { EmbeddingGenerator = embeddingGenerator });

        public override Func<IServiceCollection, IServiceCollection>[] DependencyInjectionStoreRegistrationDelegates =>
        [
            services => services
                .AddClickHouseVectorStore(_ => ClickHouseTestStore.Instance.ConnectionString)
        ];

        public override Func<IServiceCollection, IServiceCollection>[] DependencyInjectionCollectionRegistrationDelegates =>
        [
            services => services
                .AddClickHouseCollection<string, RecordWithAttributes>(
                    this.CollectionName,
                    _ => ClickHouseTestStore.Instance.ConnectionString)
        ];
    }

    public new class RomOfFloatVectorFixture : EmbeddingGenerationTests<string>.RomOfFloatVectorFixture
    {
        public override TestStore TestStore => ClickHouseTestStore.Instance;

        public override string CollectionName => "embedding_generation_rom_tests";

        public override VectorStore CreateVectorStore(IEmbeddingGenerator? embeddingGenerator)
            => new ClickHouseVectorStore(
                ClickHouseTestStore.Instance.ConnectionString,
                new() { EmbeddingGenerator = embeddingGenerator });

        public override Func<IServiceCollection, IServiceCollection>[] DependencyInjectionStoreRegistrationDelegates =>
        [
            services => services
                .AddClickHouseVectorStore(_ => ClickHouseTestStore.Instance.ConnectionString)
        ];

        public override Func<IServiceCollection, IServiceCollection>[] DependencyInjectionCollectionRegistrationDelegates =>
        [
            services => services
                .AddClickHouseCollection<string, RecordWithAttributes>(
                    this.CollectionName,
                    _ => ClickHouseTestStore.Instance.ConnectionString)
        ];
    }
}
