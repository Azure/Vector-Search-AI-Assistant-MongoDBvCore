using MongoDB.Bson;
using MongoDB.Driver;
using SharedLib.Models;
using Microsoft.Extensions.Logging;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json.Linq;
using System.Collections;

namespace SharedLib.Services;

/// <summary>
/// Service to access Azure Cosmos DB for Mongo vCore.
/// </summary>
public class MongoDbService
{
    private readonly MongoClient _client;
    private readonly IMongoDatabase _database;

    private readonly IMongoCollection<Product> _products;
    private readonly IMongoCollection<Customer> _customers;
    private readonly IMongoCollection<SalesOrder> _salesOrders;
    private readonly IMongoCollection<Session> _sessions;
    private readonly IMongoCollection<Message> _messages;
    private readonly string _vectorIndexType;
    private readonly int _maxVectorSearchResults = default;
    

    private readonly OpenAiService _openAiService;
    private readonly ILogger _logger;

    /// <summary>
    /// Creates a new instance of the service.
    /// </summary>
    /// <param name="endpoint">Endpoint URI.</param>
    /// <param name="key">Account key.</param>
    /// <param name="databaseName">Name of the database to access.</param>
    /// <param name="collectionNames">Names of the collections for this retail sample.</param>
    /// <exception cref="ArgumentNullException">Thrown when endpoint, key, databaseName, or collectionNames is either null or empty.</exception>
    /// <remarks>
    /// This constructor will validate credentials and create a service client instance.
    /// </remarks>
    public MongoDbService(string connection, string databaseName, string collectionNames, string maxVectorSearchResults, string vectorIndexType, OpenAiService openAiService, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(connection);
        ArgumentException.ThrowIfNullOrEmpty(databaseName);
        ArgumentException.ThrowIfNullOrEmpty(collectionNames);
        ArgumentException.ThrowIfNullOrEmpty(maxVectorSearchResults);
        ArgumentException.ThrowIfNullOrEmpty(vectorIndexType);


        _openAiService = openAiService;
        _logger = logger;

        _client = new MongoClient(connection);
        _database = _client.GetDatabase(databaseName);
        _maxVectorSearchResults = int.TryParse(maxVectorSearchResults, out _maxVectorSearchResults) ? _maxVectorSearchResults : 10;
        _vectorIndexType = vectorIndexType;

        _products = _database.GetCollection<Product>("products");
        _customers = _database.GetCollection<Customer>("customers");
        _salesOrders = _database.GetCollection<SalesOrder>("salesOrders");
        _sessions = _database.GetCollection<Session>("completions");
        _messages = _database.GetCollection<Message>("completions");

        
        CreateVectorIndexIfNotExists("products", _vectorIndexType);
        CreateVectorIndexIfNotExists("customers", _vectorIndexType);
        CreateVectorIndexIfNotExists("salesOrders", _vectorIndexType);
    }

    /// <summary>
    /// Create a vector index on the collection if one does not exist.
    /// </summary>
    /// <param name="collectionName">Name of the collection to create the vector index on.</param>
    /// <returns>void</returns>
    public void CreateVectorIndexIfNotExists(string collectionName, string vectorIndexType)
    {

        try
        {

            var vectorIndexDefinition = RetrieveVectorIndexDefinition(collectionName, vectorIndexType); //hnsw or ivf

            IMongoCollection<BsonDocument> collection = _database.GetCollection<BsonDocument>(collectionName);

            string vectorIndexName = "vectorSearchIndex";

            //Find if vector index exists in vectors collection
            using (IAsyncCursor<BsonDocument> indexCursor = collection.Indexes.List())
            {
                bool vectorIndexExists = indexCursor.ToList().Any(x => x["name"] == vectorIndexName);
                if (!vectorIndexExists)
                {
                    BsonDocumentCommand<BsonDocument> command = new BsonDocumentCommand<BsonDocument>(
                        vectorIndexDefinition
                    );

                    BsonDocument result = _database.RunCommand(command);
                    if (result["ok"] != 1)
                    {
                        _logger.LogError("CreateIndex failed with response: " + result.ToJson());
                    }
                }
            }

        }
        catch (MongoException ex)
        {
            _logger.LogError("MongoDbService InitializeVectorIndex: " + ex.Message);
            throw;
        }

    }

    private BsonDocument RetrieveVectorIndexDefinition(string collectionName, string vectorIndexType)
    {
        var vectorIndex = new BsonDocument();

        if(vectorIndexType == "hnsw")
        { 
            vectorIndex = new BsonDocument
            {
                { "createIndexes", collectionName },
                { "indexes", new BsonArray
                    {
                        new BsonDocument
                        {
                            { "name", "vectorSearchIndex" },
                            { "key", new BsonDocument { { "vector", "cosmosSearch" } } },
                            { "cosmosSearchOptions", new BsonDocument
                                {
                                    { "kind", "vector-hnsw" },
                                    { "m", 16 },
                                    { "efConstruction", 64 },
                                    { "similarity", "COS" },
                                    { "dimensions", 1536 }
                                }
                            }
                        }
                    }
                }
            };
        }
        else if(vectorIndexType == "ivf")
        {
            vectorIndex = new BsonDocument
            {
                { "createIndexes", collectionName },
                { "indexes", new BsonArray
                    {
                        new BsonDocument
                        {
                            { "name", "vectorSearchIndex" },
                            { "key", new BsonDocument { { "vector", "cosmosSearch" } } },
                            { "cosmosSearchOptions", new BsonDocument
                                {
                                    { "kind", "vector-ivf" },
                                    { "numLists", 2 },
                                    { "similarity", "COS" },
                                    { "dimensions", 1536 }
                                }
                            }
                        }
                    }
                }
            };
        }

        return vectorIndex;

    }

    /// <summary>
    /// Perform a vector search on the collection.
    /// </summary>
    /// <param name="collectionName">Name of the collection to execute the vector search.</param>
    /// <param name="embeddings">vectors to use in the vector search.</param>
    /// <returns>string payload of documents returned from the vector query</returns>
    public async Task<string> VectorSearchAsync(string collectionName, float[] embeddings)
    {

        string resultDocuments = string.Empty;

        
        try
        {

            IMongoCollection<BsonDocument> collection = _database.GetCollection<BsonDocument>(collectionName);

            var embeddingsArray = new BsonArray(embeddings.Select(e => new BsonDouble(Convert.ToDouble(e))));

            //Search MongoDB vCore collection for similar embeddings

            BsonDocument[] pipeline = new BsonDocument[]
            {
                new BsonDocument
                {
                    {
                        "$search", new BsonDocument
                        {
                            {
                                "cosmosSearch", new BsonDocument
                                {
                                    { "vector", embeddingsArray },
                                    { "path", "vector" },
                                    { "k", _maxVectorSearchResults }
                                }
                            },
                            { "returnStoredSource", true }
                        }
                    }
                },
                new BsonDocument
                {
                    {
                        "$project", new BsonDocument
                        {
                            { "_id", 0 },
                            { "vector", 0 }
                        }
                    }
                }
            };


            // Return results, combine into a single string
            List<BsonDocument> bsonDocuments = await collection.Aggregate<BsonDocument>(pipeline).ToListAsync();
            List<string> result = bsonDocuments.ConvertAll(bsonDocument => bsonDocument.ToString());
            resultDocuments = string.Join(" ", result);

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: VectorSearchAsync(): {ex.Message}");
            throw;
        }

        return resultDocuments;
    }

    public async Task<Product> UpsertProductAsync(Product product)
    {

        //Vectorize and add new vector property and store in vectors collection.

        try
        {

            //Serialize the product object to send to OpenAI
            string sProduct = RemoveVectorAndSerialize(product);

            (product.vector, int tokens) = await _openAiService.GetEmbeddingsAsync(string.Empty, sProduct);

            await _products.ReplaceOneAsync(
                filter: Builders<Product>.Filter.Eq("categoryId", product.categoryId)
                      & Builders<Product>.Filter.Eq("_id", product.id),
                options: new ReplaceOptions { IsUpsert = true },
                replacement: product);

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: UpsertProductAsync(): {ex.Message}");
            throw;

        }

        return product;
    }

    public async Task DeleteProductAsync(Product product)
    {

        try
        {

            var filter = Builders<Product>.Filter.And(
                 Builders<Product>.Filter.Eq("categoryId", product.categoryId),
                 Builders<Product>.Filter.Eq("_id", product.id));

            //Delete from the product collection
            await _products.DeleteOneAsync(filter);


        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: DeleteProductAsync(): {ex.Message}");
            throw;

        }

    }

    public async Task<Customer> UpsertCustomerAsync(Customer customer)
    {

        try
        {
            //Remove any existing vectors, then serialize the object to send to OpenAI
            string sObject = RemoveVectorAndSerialize(customer);

            (customer.vector, int tokens) = await _openAiService.GetEmbeddingsAsync(string.Empty, sObject);

            await _customers.ReplaceOneAsync(
                filter: Builders<Customer>.Filter.Eq("customerId", customer.customerId)
                      & Builders<Customer>.Filter.Eq("_id", customer.id),
                options: new ReplaceOptions { IsUpsert = true },
                replacement: customer);

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: UpsertCustomerAsync(): {ex.Message}");
            throw;

        }

        return customer;

    }

    public async Task DeleteCustomerAsync(Customer customer)
    {

        try
        {
            var filter = Builders<Customer>.Filter.And(
                Builders<Customer>.Filter.Eq("customerId", customer.customerId),
                Builders<Customer>.Filter.Eq("_id", customer.id));

            //Delete customer from customer collection
            await _customers.DeleteOneAsync(filter);

            
        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: DeleteCustomerAsync(): {ex.Message}");
            throw;

        }

    }

    public async Task<SalesOrder> UpsertSalesOrderAsync(SalesOrder salesOrder)
    {

        try
        {

            //Remove any existing vectors, then serialize the object to send to OpenAI
            string sObject = RemoveVectorAndSerialize(salesOrder);

            (salesOrder.vector, int tokens) = await _openAiService.GetEmbeddingsAsync(string.Empty, sObject);

            await _salesOrders.ReplaceOneAsync(
                filter: Builders<SalesOrder>.Filter.Eq("customerId", salesOrder.customerId)
                      & Builders<SalesOrder>.Filter.Eq("_id", salesOrder.id),
                options: new ReplaceOptions { IsUpsert = true },
                replacement: salesOrder);


        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: UpsertSalesOrderAsync(): {ex.Message}");
            throw;

        }

        return salesOrder;

    }

    public async Task DeleteSalesOrderAsync(SalesOrder salesOrder)
    {

        try
        {
            var filter = Builders<SalesOrder>.Filter.And(
                Builders<SalesOrder>.Filter.Eq("customerId", salesOrder.customerId),
                Builders<SalesOrder>.Filter.Eq("_id", salesOrder.id));

            await _salesOrders.DeleteOneAsync(filter);

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: DeleteSalesOrderAsync(): {ex.Message}");
            throw;

        }

    }

    private string RemoveVectorAndSerialize(object o)
    {
        string sObject = string.Empty;


        try
        {
            JObject obj = JObject.FromObject(o);

            obj.Remove("vector");

            sObject = obj.ToString();
        }
        catch {}

        return sObject;
    }

    public async Task ImportAndVectorizeAsync(string collectionName, string json)
    {
        try
        {

            IEnumerable<BsonDocument> documents = BsonSerializer.Deserialize<IEnumerable<BsonDocument>>(json);

            foreach (var document in documents)
            {
                //Vectorize item, add to vector property, save in collection.
                (float[] embeddings, int tokens) = await _openAiService.GetEmbeddingsAsync(string.Empty, document.ToString());
                
                document["vector"] = BsonValue.Create(embeddings);
                
                await _database.GetCollection<BsonDocument>(collectionName).InsertOneAsync(document);
            }

        }

        catch (MongoException ex)
        {
            _logger.LogError($"Exception: ImportJsonAsync(): {ex.Message}");
            throw;
        }
    }


    /// <summary>
    /// Gets a list of all current chat sessions.
    /// </summary>
    /// <returns>List of distinct chat session items.</returns>
    public async Task<List<Session>> GetSessionsAsync()
    {
        List<Session> sessions = new List<Session>();
        try
        {

            sessions = await _sessions.Find(
                filter: Builders<Session>.Filter.Eq("Type", nameof(Session)))
                .ToListAsync();

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: GetSessionsAsync(): {ex.Message}");
            throw;
        }

        return sessions;
    }

    /// <summary>
    /// Gets a list of all current chat messages for a specified session identifier.
    /// </summary>
    /// <param name="sessionId">Chat session identifier used to filter messages.</param>
    /// <returns>List of chat message items for the specified session.</returns>
    public async Task<List<Message>> GetSessionMessagesAsync(string sessionId)
    {
        List<Message> messages = new();

        try
        {

            messages = await _messages.Find(
                filter: Builders<Message>.Filter.Eq("Type", nameof(Message))
                & Builders<Message>.Filter.Eq("SessionId", sessionId))
                .ToListAsync();

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: GetSessionMessagesAsync(): {ex.Message}");
            throw;
        }

        return messages;

    }

    /// <summary>
    /// Creates a new chat session.
    /// </summary>
    /// <param name="session">Chat session item to create.</param>
    /// <returns>Newly created chat session item.</returns>
    public async Task InsertSessionAsync(Session session)
    {
        try
        {

            await _sessions.InsertOneAsync(session);

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: InsertSessionAsync(): {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Creates a new chat message.
    /// </summary>
    /// <param name="message">Chat message item to create.</param>
    /// <returns>Newly created chat message item.</returns>
    public async Task InsertMessageAsync(Message message)
    {
        try
        {

            await _messages.InsertOneAsync(message);

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: InsertMessageAsync(): {ex.Message}");
            throw;
        }

    }

    /// <summary>
    /// Updates an existing chat session.
    /// </summary>
    /// <param name="session">Chat session item to update.</param>
    /// <returns>Revised created chat session item.</returns>
    public async Task UpdateSessionAsync(Session session)
    {

        try
        {

            await _sessions.ReplaceOneAsync(
                filter: Builders<Session>.Filter.Eq("Type", nameof(Session))
                & Builders<Session>.Filter.Eq("SessionId", session.SessionId),
                replacement: session);

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: UpdateSessionAsync(): {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Batch create or update chat messages and session.
    /// </summary>
    /// <param name="messages">Chat message and session items to create or replace.</param>
    public async Task UpsertSessionBatchAsync(Session session, Message promptMessage, Message completionMessage)
    {
        using (var transaction = await _client.StartSessionAsync())
        {
            transaction.StartTransaction();

            try
            {

                await _sessions.ReplaceOneAsync(
                    filter: Builders<Session>.Filter.Eq("Type", nameof(Session))
                        & Builders<Session>.Filter.Eq("SessionId", session.SessionId)
                        & Builders<Session>.Filter.Eq("Id", session.Id),
                    replacement: session);

                await _messages.InsertOneAsync(promptMessage);
                await _messages.InsertOneAsync(completionMessage);

                await transaction.CommitTransactionAsync();
            }
            catch (MongoException ex)
            {
                await transaction.AbortTransactionAsync();
                _logger.LogError($"Exception: UpsertSessionBatchAsync(): {ex.Message}");
                throw;
            }
        }


    }

    /// <summary>
    /// Batch deletes an existing chat session and all related messages.
    /// </summary>
    /// <param name="sessionId">Chat session identifier used to flag messages and sessions for deletion.</param>
    public async Task DeleteSessionAndMessagesAsync(string sessionId)
    {
        try
        {

            await _database.GetCollection<BsonDocument>("completions").DeleteManyAsync(
                filter: Builders<BsonDocument>.Filter.Eq("SessionId", sessionId));

        }
        catch (MongoException ex)
        {
            _logger.LogError($"Exception: DeleteSessionAndMessagesAsync(): {ex.Message}");
            throw;
        }

    }

}