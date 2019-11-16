using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using System;

namespace MongoDbLibrary.Mongo
{
    public sealed class MongoDbClient
    {
        private readonly IMongoDatabase _db = null;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="baseName">Base name access</param>
        public MongoDbClient(string connectionString, string baseName) : this(new MongoClient(connectionString), baseName)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mongoClient">MongoClient instance.</param>
        /// <param name="mongoDbName">DataBase name access</param>
        public MongoDbClient(IMongoClient mongoClient, string mongoDbName)
        {
            if (mongoClient == null)
                throw new Exception(ResourceMessages.MESSAGE_MONGO_CLIENT_INVALID);

            if (string.IsNullOrEmpty(mongoDbName))
                throw new Exception(ResourceMessages.MESSAGE_MONGO_DB_NAME_INVALID);

            _db = mongoClient.GetDatabase(mongoDbName);

            var pack = new ConventionPack();
            pack.Add(new IgnoreExtraElementsConvention(true));
            ConventionRegistry.Register("onventions", pack, t => true);
        }
        

        /// <summary>
        /// Get collection by name
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name access</param>
        /// <returns></returns>
        public IMongoCollection<TEntity> Collection<TEntity>(string collectionName) where TEntity : class => _db.GetCollection<TEntity>(collectionName);
    }
}
