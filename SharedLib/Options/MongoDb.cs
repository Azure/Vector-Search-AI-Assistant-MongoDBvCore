using Microsoft.Extensions.Logging;
using SharedLib.Services;

namespace SharedLib.Options
{
    public record MongoDb
    {
        public string? Connection { get; set; }
        public string? DatabaseName { get; set; } 

        public string? CollectionNames { get; set; }

        public string? MaxVectorSearchResults { get; set; }

        public string? VectorIndexType { get; set; }

        public OpenAiService? OpenAiService { get; set; }

        public ILogger? Logger { get; set; }

    }
}
