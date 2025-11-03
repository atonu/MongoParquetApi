using MongoParquetApi.Models;
using MongoParquetApi.Services;

var builder = WebApplication.CreateBuilder(args);

// Services
builder.Services.AddSingleton<MongoService>();
builder.Services.AddSingleton<ParquetService>();
builder.Services.AddSingleton<ParquetSqlService>();
builder.Services.AddSingleton<CsvService>();


builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Storage selection
var storageType = builder.Configuration["Storage:Type"] ?? "Local";
if (storageType.Equals("AzureBlob", StringComparison.OrdinalIgnoreCase))
    builder.Services.AddSingleton<IFileStorage, AzureBlobStorage>();
else
    builder.Services.AddSingleton<IFileStorage, LocalFileStorage>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Basic health
app.MapGet("/", () => Results.Ok(new { ok = true, name = "MongoParquetApi" }));

// GET /api/items?name=&minPrice=&maxPrice=
app.MapGet("/api/items", async (string? name, double? minPrice, double? maxPrice, MongoService mongo, CancellationToken ct) =>
{
    var items = await mongo.GetItemsAsync(name, minPrice, maxPrice, ct);
    return Results.Ok(items);
});

// POST /api/parquet/export
// Fetches current filtered data from Mongo and writes to Parquet; returns storage path/URI
app.MapPost("/api/parquet/export", async (
    string? name,
    double? minPrice,
    double? maxPrice,
    MongoService mongo,
    ParquetService parquet,
    IFileStorage storage,
    CancellationToken ct) =>
{
    var items = await mongo.GetItemsAsync(name, minPrice, maxPrice, ct);

    var stream = await parquet.WriteItemsAsync(items, ct);
    var stamp = DateTime.UtcNow; // creation instant
    var fileName = $"items_{stamp:yyyyMMdd_HHmmss}_UTC.parquet";
    var location = await storage.SaveAsync(fileName, stream, ct);

    return Results.Ok(new
    {
        file = fileName,
        createdUtc = stamp,
        location
    });
});

// GET /api/parquet/files : list stored parquet files
app.MapGet("/api/parquet/files", async (IFileStorage storage, CancellationToken ct) =>
{
    var files = await storage.ListAsync(ct);
    return Results.Ok(files.OrderByDescending(f => f));
});

// GET /api/parquet/query?date=2025-11-02&sql=SELECT * FROM parquet WHERE Price>10
// Picks the parquet file whose name contains the given date (yyyy-MM-dd or yyyyMMdd) and runs SQL on it
app.MapGet("/api/parquet/query", async (
    string date,
    string sql,
    IFileStorage storage,
    ParquetSqlService duck,
    CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(date)) return Results.BadRequest("date is required");
    if (string.IsNullOrWhiteSpace(sql)) return Results.BadRequest("sql is required");

    var files = await storage.ListAsync(ct);
    if (files.Count == 0) return Results.NotFound("No parquet files found");

    // Accept multiple formats: yyyy-MM-dd or yyyyMMdd
    var normalized = date.Replace("-", "");
    var match = files
        .Where(f => f.Contains(normalized, StringComparison.OrdinalIgnoreCase))
        .OrderByDescending(f => f)
        .FirstOrDefault();

    if (match is null)
        return Results.NotFound($"No parquet file name contains date '{date}'");

    // Need a local path to let DuckDB read it
    string parquetPath;
    if (storage is LocalFileStorage)
    {
        // Local: open path directly
        using var _ = await storage.OpenReadAsync(match, ct); // ensure it exists
        parquetPath = Path.Combine(app.Configuration["Storage:LocalFolder"] ?? "storage/parquet", match);
    }
    else
    {
        // Azure: download to temp
        using var s = await storage.OpenReadAsync(match, ct);
        var tmp = Path.Combine(Path.GetTempPath(), match);
        using var fs = File.Create(tmp);
        await s.CopyToAsync(fs, ct);
        parquetPath = tmp;
    }

    var rows = await duck.QueryFileAsync(parquetPath, sql, ct);
    return Results.Ok(new { file = match, rows });
});


// POST /api/csv/export
// Fetches filtered Mongo data, writes CSV, stores it via IFileStorage, returns name & location
app.MapPost("/api/csv/export", async (
    string? name,
    double? minPrice,
    double? maxPrice,
    MongoService mongo,
    CsvService csvService,
    IFileStorage storage,
    CancellationToken ct) =>
{
    var items = await mongo.GetItemsAsync(name, minPrice, maxPrice, ct);

    var stream = await csvService.WriteItemsAsync(items, ct);
    var stamp = DateTime.UtcNow; // creation instant
    var fileName = $"items_{stamp:yyyyMMdd_HHmmss}_UTC.csv";
    var location = await storage.SaveAsync(fileName, stream, ct);

    return Results.Ok(new
    {
        file = fileName,
        createdUtc = stamp,
        location
    });
})
.WithName("ExportCsv")
.WithTags("CSV")
.WithSummary("Export filtered items to a CSV file and store it.");
//.WithOpenApi();


app.Run();
