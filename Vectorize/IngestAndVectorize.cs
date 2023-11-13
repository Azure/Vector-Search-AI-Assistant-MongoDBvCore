using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Net;
using Microsoft.Azure.Functions.Worker.Http;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using SharedLib.Services;


namespace Vectorize
{
    public class IngestAndVectorize
    {

        private readonly MongoDbService _mongo;
        private readonly ILogger _logger;

        public IngestAndVectorize(MongoDbService mongo, ILoggerFactory loggerFactory)
        {
            _mongo = mongo;
            _logger = loggerFactory.CreateLogger<IngestAndVectorize>();
        }

        [Function("IngestAndVectorize")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", "get", Route = null)] HttpRequestData req)
        {
            _logger.LogInformation("Ingest and Vectorize HTTP trigger function is processing a request.");
            try
            {
                
                // Ingest json data into MongoDB collections
                await IngestDataFromBlobStorageAsync();


                var response = req.CreateResponse(HttpStatusCode.OK);
                response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
                await response.WriteStringAsync("Ingest and Vectorize HTTP trigger function executed successfully.");

                return response;
            }
            catch (Exception ex)
            {

                var response = req.CreateResponse(HttpStatusCode.BadRequest);
                await response.WriteStringAsync(ex.ToString());
                return response;

            }
        }

        public async Task IngestDataFromBlobStorageAsync()
        {
            

            try
            {
                BlobContainerClient blobContainerClient = new BlobContainerClient(new Uri("https://cosmosdbcosmicworks.blob.core.windows.net/cosmic-works-mongo-vcore/"));

                //hard-coded here.  In a real-world scenario, you would want to dynamically get the list of blobs in the container and iterate through them.
                //as well as drive all of the schema and meta-data from a configuration file.
                List<string> blobIds = new List<string>() { "products", "customers", "salesOrders" };


                foreach(string blobId in blobIds)
                {
                    BlobClient blob = blobContainerClient.GetBlobClient($"{blobId}.json");
                    if (await blob.ExistsAsync())
                    {
                        //Download and ingest products.json
                        _logger.LogInformation($"Ingesting {blobId} data from blob storage.");

                        BlobClient blobClient = blobContainerClient.GetBlobClient($"{blobId}.json");
                        BlobDownloadStreamingResult blobResult = await blobClient.DownloadStreamingAsync();

                        using (StreamReader pReader = new StreamReader(blobResult.Content))
                        {
                            string json = await pReader.ReadToEndAsync();
                            await _mongo.ImportAndVectorizeAsync(blobId, json);

                        }
                        
                        _logger.LogInformation($"{blobId} data ingestion complete.");

                    }
                }

            }
            catch(Exception ex)
            {
                _logger.LogError($"Exception: IngestDataFromBlobStorageAsync(): {ex.Message}");
                throw;
            }
        }
    }
}
