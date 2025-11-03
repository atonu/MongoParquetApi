using System.Text;

namespace MongoParquetApi.Services;

public sealed class LocalFileStorage : IFileStorage
{
    private readonly string _root;

    public LocalFileStorage(IConfiguration cfg)
    {
        _root = cfg["Storage:LocalFolder"] ?? "storage/parquet";
        Directory.CreateDirectory(_root);
    }

    public async Task<string> SaveAsync(string fileName, Stream content, CancellationToken ct = default)
    {
        var path = Path.Combine(_root, fileName);
        using var fs = File.Create(path);
        await content.CopyToAsync(fs, ct);
        return path;
    }

    public Task<Stream> OpenReadAsync(string fileName, CancellationToken ct = default)
    {
        var path = Path.Combine(_root, fileName);
        Stream s = File.OpenRead(path);
        return Task.FromResult(s);
    }

    public Task<IReadOnlyList<string>> ListAsync(CancellationToken ct = default)
    {
        var files = Directory.EnumerateFiles(_root, "*.parquet").Select(Path.GetFileName).ToList();
        return Task.FromResult((IReadOnlyList<string>)files);
    }
}
