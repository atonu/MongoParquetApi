using Parquet;
using Parquet.Schema;
using MongoParquetApi.Models;

namespace MongoParquetApi.Services;

public sealed class ParquetService
{
    public async Task<Stream> WriteItemsAsync(IEnumerable<Item> items, CancellationToken ct = default)
    {
        // Define Parquet schema
        var schema = new ParquetSchema(
            new DataField<string>("Id"),
            new DataField<string>("Name"),
            new DataField<double>("Price"),
            new DataField<DateTime>("CreatedAtUtc")
        );

        // Materialize columns
        var data = items.ToList();
        var idCol = data.Select(x => x.Id).ToArray();
        var nameCol = data.Select(x => x.Name).ToArray();
        var priceCol = data.Select(x => x.Price).ToArray();
        var created = data.Select(x => x.CreatedAtUtc).ToArray();

        var ms = new MemoryStream();
        using (var writer = await ParquetWriter.CreateAsync(schema, ms, cancellationToken: ct))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteColumnAsync(new Parquet.Data.DataColumn((DataField)schema[0], idCol), ct);
            await group.WriteColumnAsync(new Parquet.Data.DataColumn((DataField)schema[1], nameCol), ct);
            await group.WriteColumnAsync(new Parquet.Data.DataColumn((DataField)schema[2], priceCol), ct);
            await group.WriteColumnAsync(new Parquet.Data.DataColumn((DataField)schema[3], created), ct);
        }
        ms.Position = 0;
        return ms;
    }
}
