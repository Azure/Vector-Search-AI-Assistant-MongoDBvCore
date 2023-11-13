using Humanizer.Localisation.TimeToClockNotation;
using Search.Constants;
using SharedLib.Models;
using SharedLib.Services;
using SharpToken;
using Microsoft.ML.Tokenizers;

namespace Search.Services;

public class ChatService
{
    /// <summary>
    /// All data is cached in the _sessions List object.
    /// </summary>
    private static List<Session> _sessions = new();

    private readonly OpenAiService _openAiService;
    private readonly MongoDbService _mongoDbService;
    private readonly int _maxConversationTokens;
    private readonly int _maxCompletionTokens;
    private readonly ILogger _logger;

    public ChatService(OpenAiService openAiService, MongoDbService mongoDbService, ILogger logger)
    {

        _openAiService = openAiService;
        _mongoDbService = mongoDbService;

        _maxConversationTokens = openAiService.MaxConversationTokens;
        _maxCompletionTokens = openAiService.MaxCompletionTokens;
        _logger = logger;
    }

    /// <summary>
    /// Returns list of chat session ids and names for left-hand nav to bind to (display Name and ChatSessionId as hidden)
    /// </summary>
    public async Task<List<Session>> GetAllChatSessionsAsync()
    {  
        return _sessions = await _mongoDbService.GetSessionsAsync();
    }

    /// <summary>
    /// Returns the chat messages to display on the main web page when the user selects a chat from the left-hand nav
    /// </summary>
    public async Task<List<Message>> GetChatSessionMessagesAsync(string? sessionId)
    {
        ArgumentNullException.ThrowIfNull(sessionId);

        List<Message> chatMessages = new();

        if (_sessions.Count == 0)
        {
            return Enumerable.Empty<Message>().ToList();
        }

        int index = _sessions.FindIndex(s => s.SessionId == sessionId);

        if (_sessions[index].Messages.Count == 0)
        {
            // Messages are not cached, go read from database
            chatMessages = await _mongoDbService.GetSessionMessagesAsync(sessionId);

            // Cache results
            _sessions[index].Messages = chatMessages;
        }
        else
        {
            // Load from cache
            chatMessages = _sessions[index].Messages;
        }

        return chatMessages;
    }

    /// <summary>
    /// User creates a new Chat Session.
    /// </summary>
    public async Task CreateNewChatSessionAsync()
    {
        Session session = new();

        _sessions.Add(session);

        await _mongoDbService.InsertSessionAsync(session);

    }

    /// <summary>
    /// Rename the Chat Ssssion from "New Chat" to the summary provided by OpenAI
    /// </summary>
    public async Task RenameChatSessionAsync(string? sessionId, string newChatSessionName)
    {
        ArgumentNullException.ThrowIfNull(sessionId);

        int index = _sessions.FindIndex(s => s.SessionId == sessionId);

        _sessions[index].Name = newChatSessionName;

        await _mongoDbService.UpdateSessionAsync(_sessions[index]);
    }

    /// <summary>
    /// User deletes a chat session
    /// </summary>
    public async Task DeleteChatSessionAsync(string? sessionId)
    {
        ArgumentNullException.ThrowIfNull(sessionId);

        int index = _sessions.FindIndex(s => s.SessionId == sessionId);

        _sessions.RemoveAt(index);

        await _mongoDbService.DeleteSessionAndMessagesAsync(sessionId);
    }

    /// <summary>
    /// Receive a prompt from a user, Vectorize it from _openAIService Get a completion from _openAiService
    /// </summary>
    public async Task<string> GetChatCompletionAsync(string? sessionId, string userPrompt, string collectionName)
    {

        try
        { 
            ArgumentNullException.ThrowIfNull(sessionId);


            //Get embeddings for user prompt and number of tokens it uses.
            (float[] promptVectors, int promptTokens) = await _openAiService.GetEmbeddingsAsync(sessionId, userPrompt);
            //Create the prompt message object. Created here to give it a timestamp that precedes the completion message.
            Message promptMessage = new Message(sessionId, nameof(Participants.User), promptTokens, default, userPrompt);


            //Do vector search on the user prompt, return list of documents
            string retrievedDocuments = await _mongoDbService.VectorSearchAsync(collectionName, promptVectors);

            //Get the most recent conversation history up to _maxConversationTokens
            string conversation = GetConversationHistory(sessionId);


            //Construct our prompts sent to Azure OpenAI. Calculate token usage and trim the RAG payload and conversation history to prevent exceeding token limits.
            (string augmentedContent, string conversationAndUserPrompt) = BuildPrompts(userPrompt, conversation, retrievedDocuments);


            //Generate the completion from Azure OpenAI to return to the user
            (string completionText, int ragTokens, int completionTokens) = await _openAiService.GetChatCompletionAsync(sessionId, conversationAndUserPrompt, augmentedContent);


            //Create the completion message object
            Message completionMessage = new Message(sessionId, nameof(Participants.Assistant), completionTokens, ragTokens, completionText);

            //Add the user prompt and completion to cache, then persist to Cosmos in a transaction
            await AddPromptCompletionMessagesAsync(sessionId, promptMessage, completionMessage);


            return completionText;

        }
        catch (Exception ex) 
        {
            string message = $"ChatService.GetChatCompletionAsync(): {ex.Message}";
            _logger.LogError(message);
            throw;

        }
    }

    /// <summary>
    /// Estimate the token usage for OpenAI completion to prevent exceeding the OpenAI model's maximum token limit. This function estimates the
    /// amount of tokens the vector search result data and the user prompt will consume. If the search result data exceeds the configured amount
    /// the function reduces the number of vectors, reducing the amount of data sent.
    /// </summary>
    private (string augmentedContent, string conversationAndUserPrompt) BuildPrompts(string userPrompt, string conversation, string retrievedData)
    {
        
        string updatedAugmentedContent = "";
        string updatedConversationAndUserPrompt = "";

        
        //SharpToken only estimates token usage and often undercounts. Add a buffer of 200 tokens.
        int bufferTokens = 200;

        //Create a new instance of SharpToken
        var encoding = GptEncoding.GetEncoding("cl100k_base");  //encoding base for GPT 3.5 Turbo and GPT 4
        //var encoding = GptEncoding.GetEncodingForModel("gpt-35-turbo");

        List<int> ragVectors = encoding.Encode(retrievedData);
        int ragTokens = ragVectors.Count;

        List<int> convVectors = encoding.Encode(conversation);
        int convTokens = convVectors.Count;

        int userPromptTokens = encoding.Encode(userPrompt).Count;


        //If RAG data plus user prompt, plus conversation, plus tokens for completion is greater than max completion tokens we've defined, reduce the rag data and conversation by relative amount.
        int totalTokens = ragTokens + convTokens + userPromptTokens + bufferTokens;

        //Too much data, reduce the rag data and conversation data by the same percentage. Do not reduce the user prompt as this is required for the completion.
        if (totalTokens > _maxCompletionTokens)
        {
            //Get the number of tokens to reduce by
            int tokensToReduce = totalTokens - _maxCompletionTokens;

            //Get the percentage of tokens to reduce by
            float ragTokenPct = (float)ragTokens / totalTokens;
            float conTokenPct = (float)convTokens / totalTokens;

            //Calculate the new number of tokens for each data set
            int newRagTokens = (int)Math.Round(ragTokens - (ragTokenPct * tokensToReduce), 0);
            int newConvTokens = (int)Math.Round(convTokens - (conTokenPct * tokensToReduce), 0);

            
            //Get the reduced set of RAG vectors
            List<int> trimmedRagVectors = ragVectors.GetRange(0, newRagTokens);
            //Convert the vectors back to text
            updatedAugmentedContent = encoding.Decode(trimmedRagVectors);

            int offset = convVectors.Count - newConvTokens;

            //Get the reduce set of conversation vectors
            List<int> trimmedConvVectors = convVectors.GetRange(offset, newConvTokens);
            
            //Convert vectors back into reduced conversation length
            updatedConversationAndUserPrompt = encoding.Decode(trimmedConvVectors);

            //add user prompt
            updatedConversationAndUserPrompt += Environment.NewLine + userPrompt;


        }
        //If everything is less than _maxCompletionTokens then good to go.
        else
        {
            
            //Return all of the content
            updatedAugmentedContent = retrievedData;
            updatedConversationAndUserPrompt = conversation + Environment.NewLine + userPrompt;
        }


        return (augmentedContent: updatedAugmentedContent, conversationAndUserPrompt: updatedConversationAndUserPrompt);

    }

    /// <summary>
    /// Get the most recent conversation history to provide additional context for the completion LLM
    /// </summary>
    private string GetConversationHistory(string sessionId)
    {

        int? tokensUsed = 0;
        
        int index = _sessions.FindIndex(s => s.SessionId == sessionId);

        List<Message> conversationMessages = _sessions[index].Messages.ToList(); //make a full copy

        //Iterate through these in reverse order to get the most recent conversation history up to _maxConversationTokens
        var trimmedMessages = conversationMessages
            .OrderByDescending(m => m.TimeStamp)
            .TakeWhile(m => (tokensUsed += m.Tokens) <= _maxConversationTokens)
            .Select(m => m.Text)
            .ToList();

        trimmedMessages.Reverse();

        //Return as a string
        string conversation = string.Join(Environment.NewLine, trimmedMessages.ToArray());

        return conversation;

    }

    public async Task<string> SummarizeChatSessionNameAsync(string? sessionId, string prompt)
    {
        ArgumentNullException.ThrowIfNull(sessionId);

        string response = await _openAiService.SummarizeAsync(sessionId, prompt);

        await RenameChatSessionAsync(sessionId, response);

        return response;
    }

    /// <summary>
    /// Add user prompt to the chat session message list object and insert into the data service.
    /// </summary>
    private async Task AddPromptMessageAsync(string sessionId, string promptText)
    {
        Message promptMessage = new(sessionId, nameof(Participants.User), default, default, promptText);

        int index = _sessions.FindIndex(s => s.SessionId == sessionId);

        _sessions[index].AddMessage(promptMessage);

        await _mongoDbService.InsertMessageAsync(promptMessage);
    }


    /// <summary>
    /// Add user prompt and AI assistance response to the chat session message list object and insert into the data service as a transaction.
    /// </summary>
    private async Task AddPromptCompletionMessagesAsync(string sessionId, Message promptMessage, Message completionMessage)
    {

        int index = _sessions.FindIndex(s => s.SessionId == sessionId);


        //Add prompt and completion to the cache
        _sessions[index].AddMessage(promptMessage);
        _sessions[index].AddMessage(completionMessage);


        //Update session cache with tokens used
        _sessions[index].TokensUsed += promptMessage.Tokens;
        _sessions[index].TokensUsed += completionMessage.PromptTokens;
        _sessions[index].TokensUsed += completionMessage.Tokens;

        await _mongoDbService.UpsertSessionBatchAsync(session: _sessions[index], promptMessage: promptMessage, completionMessage: completionMessage);

    }
}