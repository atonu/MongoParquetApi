using MongoDB.Bson;
using MongoDB.Driver;
using MongoParquetApi.Models;

namespace MongoParquetApi.Services;

public sealed class MongoService
{
    private readonly IMongoCollection<BsonDocument> _col;

    public MongoService(IConfiguration cfg)
    {
        var cs = cfg["Mongo:ConnectionString"] ?? "mongodb://localhost:27017";
        var db = cfg["Mongo:Database"] ?? "sampledb";
        var col = cfg["Mongo:Collection"] ?? "items";

        var client = new MongoClient(cs);
        _col = client.GetDatabase(db).GetCollection<BsonDocument>(col);
    }

    public async Task<IReadOnlyList<Item>> GetItemsAsync(
        string? name = null, double? minPrice = null, double? maxPrice = null, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Empty;

        if (!string.IsNullOrWhiteSpace(name))
            filter &= Builders<BsonDocument>.Filter.Regex("Name", new BsonRegularExpression(name, "i"));

        if (minPrice.HasValue)
            filter &= Builders<BsonDocument>.Filter.Gte("Price", minPrice.Value);

        if (maxPrice.HasValue)
            filter &= Builders<BsonDocument>.Filter.Lte("Price", maxPrice.Value);

        var docs = await _col.Find(filter).ToListAsync(ct);

        return docs.Select(d => new Item(
            Id: (d.TryGetValue("_id", out var id) ? id.ToString() : Guid.NewGuid().ToString()),
            Name: d.TryGetValue("Name", out var n) ? n.AsString : "",
            Price: d.TryGetValue("Price", out var p) ? p.ToDouble() : 0d,
            CreatedAtUtc: d.TryGetValue("CreatedAtUtc", out var c)
                ? (c.IsValidDateTime ? c.ToUniversalTime() : DateTime.UtcNow)
                : DateTime.UtcNow
        )).ToList();
    }
}
