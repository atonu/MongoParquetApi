using System.Globalization;
using CsvHelper;
using MongoParquetApi.Models;

namespace MongoParquetApi.Services;

public sealed class CsvService
{
    public async Task<Stream> WriteItemsAsync(IEnumerable<Item> items, CancellationToken ct = default)
    {
        var ms = new MemoryStream();
        using (var writer = new StreamWriter(ms, new System.Text.UTF8Encoding(false), leaveOpen: true))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            // If you want specific column order/headers:
            csv.WriteField("Id");
            csv.WriteField("Name");
            csv.WriteField("Price");
            csv.WriteField("CreatedAtUtc");
            await csv.NextRecordAsync();

            foreach (var i in items)
            {
                csv.WriteField(i.Id);
                csv.WriteField(i.Name);
                csv.WriteField(i.Price);
                // ISO 8601
                csv.WriteField(i.CreatedAtUtc.ToString("O"));
                await csv.NextRecordAsync();
            }
            await writer.FlushAsync();
        }
        ms.Position = 0;
        return ms;
    }
}
