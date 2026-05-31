using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using CsvHelper;
using CsvHelper.Configuration.Attributes;

var bootstrapServers = Env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
var topic = Env("KAFKA_TOPIC", "sales");
var dataDir = Env("DATA_DIR", "/data");
var delayMs = int.Parse(Env("PRODUCER_DELAY_MS", "5"), CultureInfo.InvariantCulture);
var maxRows = int.Parse(Env("MAX_ROWS", "0"), CultureInfo.InvariantCulture);

if (!Directory.Exists(dataDir))
{
    throw new DirectoryNotFoundException($"CSV directory was not found: {dataDir}");
}

await EnsureTopicAsync(bootstrapServers, topic);

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.All,
    EnableIdempotence = true,
    MessageSendMaxRetries = 5
};

var jsonOptions = new JsonSerializerOptions
{
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
};

using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

var files = Directory
    .EnumerateFiles(dataDir, "*.csv")
    .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
    .ToArray();

var sent = 0;
Console.WriteLine($"ABDLab3 producer started. Files: {files.Length}, topic: {topic}");

foreach (var file in files)
{
    using var reader = new StreamReader(file);
    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    var rowNumber = 0;

    await foreach (var row in csv.GetRecordsAsync<SaleCsvRow>())
    {
        rowNumber++;
        row.SourceFile = Path.GetFileName(file);
        row.SourceRowNumber = rowNumber;

        var key = $"{row.SourceFile}:{row.Id}";
        var payload = JsonSerializer.Serialize(row, jsonOptions);

        await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = payload });
        sent++;

        if (sent % 1000 == 0)
        {
            Console.WriteLine($"Sent {sent} messages");
        }

        if (delayMs > 0)
        {
            await Task.Delay(delayMs);
        }

        if (maxRows > 0 && sent >= maxRows)
        {
            break;
        }
    }

    if (maxRows > 0 && sent >= maxRows)
    {
        break;
    }
}

producer.Flush(TimeSpan.FromSeconds(30));
Console.WriteLine($"ABDLab3 producer finished. Sent messages: {sent}");

static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;

static async Task EnsureTopicAsync(string bootstrapServers, string topic)
{
    using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

    try
    {
        await admin.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 3,
                ReplicationFactor = 1
            }
        });
    }
    catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
    {
    }
}

public sealed class SaleCsvRow
{
    [Name("id")]
    [JsonPropertyName("id")]
    public long Id { get; set; }

    [Ignore]
    [JsonPropertyName("source_file")]
    public string SourceFile { get; set; } = "";

    [Ignore]
    [JsonPropertyName("source_row_number")]
    public int SourceRowNumber { get; set; }

    [Name("customer_first_name")]
    [JsonPropertyName("customer_first_name")]
    public string? CustomerFirstName { get; set; }

    [Name("customer_last_name")]
    [JsonPropertyName("customer_last_name")]
    public string? CustomerLastName { get; set; }

    [Name("customer_age")]
    [JsonPropertyName("customer_age")]
    public int? CustomerAge { get; set; }

    [Name("customer_email")]
    [JsonPropertyName("customer_email")]
    public string? CustomerEmail { get; set; }

    [Name("customer_country")]
    [JsonPropertyName("customer_country")]
    public string? CustomerCountry { get; set; }

    [Name("customer_postal_code")]
    [JsonPropertyName("customer_postal_code")]
    public string? CustomerPostalCode { get; set; }

    [Name("customer_pet_type")]
    [JsonPropertyName("customer_pet_type")]
    public string? CustomerPetType { get; set; }

    [Name("customer_pet_name")]
    [JsonPropertyName("customer_pet_name")]
    public string? CustomerPetName { get; set; }

    [Name("customer_pet_breed")]
    [JsonPropertyName("customer_pet_breed")]
    public string? CustomerPetBreed { get; set; }

    [Name("seller_first_name")]
    [JsonPropertyName("seller_first_name")]
    public string? SellerFirstName { get; set; }

    [Name("seller_last_name")]
    [JsonPropertyName("seller_last_name")]
    public string? SellerLastName { get; set; }

    [Name("seller_email")]
    [JsonPropertyName("seller_email")]
    public string? SellerEmail { get; set; }

    [Name("seller_country")]
    [JsonPropertyName("seller_country")]
    public string? SellerCountry { get; set; }

    [Name("seller_postal_code")]
    [JsonPropertyName("seller_postal_code")]
    public string? SellerPostalCode { get; set; }

    [Name("product_name")]
    [JsonPropertyName("product_name")]
    public string? ProductName { get; set; }

    [Name("product_category")]
    [JsonPropertyName("product_category")]
    public string? ProductCategory { get; set; }

    [Name("product_price")]
    [JsonPropertyName("product_price")]
    public decimal? ProductPrice { get; set; }

    [Name("product_quantity")]
    [JsonPropertyName("product_quantity")]
    public int? ProductQuantity { get; set; }

    [Name("sale_date")]
    [JsonPropertyName("sale_date")]
    public string? SaleDate { get; set; }

    [Name("sale_customer_id")]
    [JsonPropertyName("sale_customer_id")]
    public long? SaleCustomerId { get; set; }

    [Name("sale_seller_id")]
    [JsonPropertyName("sale_seller_id")]
    public long? SaleSellerId { get; set; }

    [Name("sale_product_id")]
    [JsonPropertyName("sale_product_id")]
    public long? SaleProductId { get; set; }

    [Name("sale_quantity")]
    [JsonPropertyName("sale_quantity")]
    public int? SaleQuantity { get; set; }

    [Name("sale_total_price")]
    [JsonPropertyName("sale_total_price")]
    public decimal? SaleTotalPrice { get; set; }

    [Name("store_name")]
    [JsonPropertyName("store_name")]
    public string? StoreName { get; set; }

    [Name("store_location")]
    [JsonPropertyName("store_location")]
    public string? StoreLocation { get; set; }

    [Name("store_city")]
    [JsonPropertyName("store_city")]
    public string? StoreCity { get; set; }

    [Name("store_state")]
    [JsonPropertyName("store_state")]
    public string? StoreState { get; set; }

    [Name("store_country")]
    [JsonPropertyName("store_country")]
    public string? StoreCountry { get; set; }

    [Name("store_phone")]
    [JsonPropertyName("store_phone")]
    public string? StorePhone { get; set; }

    [Name("store_email")]
    [JsonPropertyName("store_email")]
    public string? StoreEmail { get; set; }

    [Name("pet_category")]
    [JsonPropertyName("pet_category")]
    public string? PetCategory { get; set; }

    [Name("product_weight")]
    [JsonPropertyName("product_weight")]
    public decimal? ProductWeight { get; set; }

    [Name("product_color")]
    [JsonPropertyName("product_color")]
    public string? ProductColor { get; set; }

    [Name("product_size")]
    [JsonPropertyName("product_size")]
    public string? ProductSize { get; set; }

    [Name("product_brand")]
    [JsonPropertyName("product_brand")]
    public string? ProductBrand { get; set; }

    [Name("product_material")]
    [JsonPropertyName("product_material")]
    public string? ProductMaterial { get; set; }

    [Name("product_description")]
    [JsonPropertyName("product_description")]
    public string? ProductDescription { get; set; }

    [Name("product_rating")]
    [JsonPropertyName("product_rating")]
    public decimal? ProductRating { get; set; }

    [Name("product_reviews")]
    [JsonPropertyName("product_reviews")]
    public int? ProductReviews { get; set; }

    [Name("product_release_date")]
    [JsonPropertyName("product_release_date")]
    public string? ProductReleaseDate { get; set; }

    [Name("product_expiry_date")]
    [JsonPropertyName("product_expiry_date")]
    public string? ProductExpiryDate { get; set; }

    [Name("supplier_name")]
    [JsonPropertyName("supplier_name")]
    public string? SupplierName { get; set; }

    [Name("supplier_contact")]
    [JsonPropertyName("supplier_contact")]
    public string? SupplierContact { get; set; }

    [Name("supplier_email")]
    [JsonPropertyName("supplier_email")]
    public string? SupplierEmail { get; set; }

    [Name("supplier_phone")]
    [JsonPropertyName("supplier_phone")]
    public string? SupplierPhone { get; set; }

    [Name("supplier_address")]
    [JsonPropertyName("supplier_address")]
    public string? SupplierAddress { get; set; }

    [Name("supplier_city")]
    [JsonPropertyName("supplier_city")]
    public string? SupplierCity { get; set; }

    [Name("supplier_country")]
    [JsonPropertyName("supplier_country")]
    public string? SupplierCountry { get; set; }
}
