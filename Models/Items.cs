namespace MongoParquetApi.Models;

public sealed record Item(
    string Id,
    string Name,
    double Price,
    DateTime CreatedAtUtc
);
