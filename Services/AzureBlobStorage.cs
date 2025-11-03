using Azure.Storage.Blobs;

namespace MongoParquetApi.Services;

public sealed class AzureBlobStorage : IFileStorage
{
    private readonly BlobContainerClient _container;

    public AzureBlobStorage(IConfiguration cfg)
    {
        var cs = cfg["Storage:Azure:ConnectionString"] ?? throw new InvalidOperationException("Azure CS missing");
        var container = cfg["Storage:Azure:Container"] ?? "parquetfiles";
        _container = new BlobContainerClient(cs, container);
        _container.CreateIfNotExists();
    }

    public async Task<string> SaveAsync(string fileName, Stream content, CancellationToken ct = default)
    {
        var blob = _container.GetBlobClient(fileName);
        content.Position = 0;
        await blob.UploadAsync(content, overwrite: true, cancellationToken: ct);
        return blob.Uri.ToString();
    }

    public async Task<Stream> OpenReadAsync(string fileName, CancellationToken ct = default)
    {
        var blob = _container.GetBlobClient(fileName);
        var dl = await blob.DownloadStreamingAsync(cancellationToken: ct);
        return dl.Value.Content;
    }

    public async Task<IReadOnlyList<string>> ListAsync(CancellationToken ct = default)
    {
        var list = new List<string>();
        await foreach (var b in _container.GetBlobsAsync(cancellationToken: ct))
            if (b.Name.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
                list.Add(b.Name);
        return list;
    }
}
