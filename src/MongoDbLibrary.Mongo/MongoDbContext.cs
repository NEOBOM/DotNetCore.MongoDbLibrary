using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDbLibrary.Mongo
{
    public abstract class MongoDbContext
    {
        private readonly MongoDbClient _mongoDbClient = null;

        public MongoDbContext(MongoDbClient mongoDbClient)
        {
            _mongoDbClient = mongoDbClient;
        }

        /// <summary>
        /// Find a document
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <returns>TEntity</returns>
        public TEntity Find<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).FirstOrDefault();
        }

        /// <summary>
        /// Find a document async
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <returns></returns>
        public Task<TEntity> FindAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).FirstOrDefaultAsync();
        }

        /// <summary>
        /// Find all documentos by filter.
        /// </summary>
        /// <typeparam name="TEntity">Entity type return</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <returns></returns>
        public List<TEntity> Select<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).ToList();
        }

        /// <summary>
        /// Find all documentos by filter.
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <returns>Task<List<TEntity>></returns>
        public Task<List<TEntity>> SelectAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).ToListAsync();
        }

        /// <summary>
        /// Find all documentos by filters.
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <typeparam name="T">Filter type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="entities">Entities for filter</param>
        /// <returns>Task<List<TEntity>></returns>
        public Task<List<TEntity>> Select<TEntity, T>(string collectionName, Func<T, Expression<Func<TEntity, bool>>> expression, List<T> entities) where TEntity : class where T : class
        {
            var filters = Builders<TEntity>.Filter.And(entities?.Select(entity => Builders<TEntity>.Filter.Where(expression(entity))));
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filters).ToListAsync();
        }

        /// <summary>
        /// Find all documentos by filters.
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <typeparam name="T">Filter type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="entities">Entities for filter</param>
        /// <returns>Task<List<TEntity>></returns>
        public Task<List<TEntity>> SelectAsync<TEntity, T>(string collectionName, Func<T, Expression<Func<TEntity, bool>>> expression, List<T> entities) where TEntity : class where T : class
        {
            var filters = Builders<TEntity>.Filter.And(entities?.Select(entity => Builders<TEntity>.Filter.Where(expression(entity))));
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filters).ToListAsync();
        }

        /// <summary>
        /// Find all documentos by filters with limit.
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="limit">Limit the mumber of documents</param>
        /// <returns></returns>
        public List<TEntity> SelectWithLimit<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, int limit) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).Limit(limit).ToList();
        }

        /// <summary>
        /// Find all documentos by filters with limit.
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="limit">Limit the mumber of documents</param>
        /// <returns>Task<List<TEntity>></returns>
        public Task<List<TEntity>> SelectWithLimitAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, int limit) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).Limit(limit).ToListAsync();
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public List<TEntity> SelectWithKey<TEntity>(string collectionName, string key, string value) where TEntity : class
        {
            var filter = Builders<TEntity>.Filter.Eq(key, value);
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filter).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Task<List<TEntity>> SelectWithKeyAsync<TEntity>(string collectionName, string key, string value) where TEntity : class
        {
            var filter = Builders<TEntity>.Filter.Eq(key, value);
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filter).ToListAsync();
        }

        /// <summary>
        /// Get all documentos.
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <returns>List<TEntity></returns>
        public List<TEntity> SelectAll<TEntity>(string collectionName) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(_ => true).ToList();
        }

        /// <summary>
        /// Get all documentos.
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <returns>Task<List<TEntity>></returns>
        public Task<List<TEntity>> SelectAllAsync<TEntity>(string collectionName) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(_ => true).ToListAsync();
        }

        /// <summary>
        /// Insert one document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="entity">Entity inserted</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        public void InsertOne<TEntity>(string collectionName, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            _mongoDbClient.Collection<TEntity>(collectionName).InsertOne(entity, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Insert one document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="entity">Entity to insered</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task</returns>
        public Task InsertOneAsync<TEntity>(string collectionName, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).InsertOneAsync(entity, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Insert many documents.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="entities">Entities to insered</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        public void InsertMany<TEntity>(string collectionName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default) where TEntity : class
        {
            _mongoDbClient.Collection<TEntity>(collectionName).InsertMany(entities, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Insert many documents.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="entities">Entities to insered</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task</returns>
        public Task InsertManyAsync<TEntity>(string collectionName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).InsertManyAsync(entities, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Replace one document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="entity">Entity to replace</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>ReplaceOneResult</returns>
        public ReplaceOneResult ReplaceOne<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).ReplaceOne(expression, entity, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Replace one document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="entity">Entity to replace</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task<ReplaceOneResult></returns>
        public Task<ReplaceOneResult> ReplaceOneAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).ReplaceOneAsync(expression, entity, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Update one push document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="field">Field updated</param>
        /// <param name="entity">Entity to updated</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>UpdateResult</returns>
        public UpdateResult UpdateOnePush<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, string field, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            var update = Builders<TEntity>.Update.Push(field, entity);
            return _mongoDbClient.Collection<TEntity>(collectionName).UpdateOne(expression, update, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Update one push document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="field">Field updated</param>
        /// <param name="entity">Entity to updated</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task<UpdateResult></returns>
        public Task<UpdateResult> UpdateOnePushAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, string field, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            var update = Builders<TEntity>.Update.Push(field, entity);
            return _mongoDbClient.Collection<TEntity>(collectionName).UpdateOneAsync(expression, update, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Update one AddToSet document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="field">Field updated</param>
        /// <param name="entity">Entity to updated</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task<UpdateResult></returns>
        public Task<UpdateResult> UpdateOneAddToSet<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, string field, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            var update = Builders<TEntity>.Update.AddToSet(field, entity);
            return _mongoDbClient.Collection<TEntity>(collectionName).UpdateOneAsync(expression, update, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Update one AddToSet document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="field">Field updated</param>
        /// <param name="entity">Entity to updated</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task<UpdateResult></returns>
        public Task<UpdateResult> UpdateOneAddToSetAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, string field, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        {
            var update = Builders<TEntity>.Update.AddToSet(field, entity);
            return _mongoDbClient.Collection<TEntity>(collectionName).UpdateOneAsync(expression, update, cancellationToken: cancellationToken);
        }
        

        /// <summary>
        /// BulkWrite documents
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="entities">Entities to insered or updated</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>BulkWriteResult<TEntity></returns>
        public BulkWriteResult<TEntity> BulkWrite<TEntity>(string collectionName, Func<TEntity, Expression<Func<TEntity, bool>>> expression, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default) where TEntity : class
        {
            var bulkOps = entities?.Select(entity => new ReplaceOneModel<TEntity>(expression(entity), entity) { IsUpsert = true });
            return _mongoDbClient.Collection<TEntity>(collectionName).BulkWrite(bulkOps, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// BulkWrite documents
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="entities">Entities to insered or updated</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task<BulkWriteResult<TEntity>></returns>
        public Task<BulkWriteResult<TEntity>> BulkWriteAsync<TEntity>(string collectionName, Func<TEntity, Expression<Func<TEntity, bool>>> expression, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default) where TEntity : class
        {
            var bulkOps = entities?.Select(entity => new ReplaceOneModel<TEntity>(expression(entity), entity) { IsUpsert = true });
            return _mongoDbClient.Collection<TEntity>(collectionName).BulkWriteAsync(bulkOps, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Delete one document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>DeleteResult</returns>
        public DeleteResult DeleteOne<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).DeleteOne(expression, cancellationToken);
        }

        /// <summary>
        /// Delete one document.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task<DeleteResult></returns>
        public Task<DeleteResult> DeleteOneAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).DeleteOneAsync(expression, cancellationToken);
        }

        /// <summary>
        /// Delete many documents.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>DeleteResult</returns>
        public DeleteResult DeleteMany<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).DeleteMany(expression, cancellationToken);
        }

        /// <summary>
        /// Delete many documents.
        /// </summary>
        /// <typeparam name="TEntity">Entity type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>Task<DeleteResult></DeleteResult></returns>
        public Task<DeleteResult> DeleteManyAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).DeleteManyAsync(expression, cancellationToken);
        }
    }
}
