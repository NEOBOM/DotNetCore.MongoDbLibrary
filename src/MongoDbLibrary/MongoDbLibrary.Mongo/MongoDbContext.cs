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
        /// <param name="expression">Expression for find documento</param>
        /// <returns></returns>
        public TEntity Find<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).FirstOrDefault();
        }

        /// <summary>
        /// Find a document async
        /// </summary>
        /// <typeparam name="TEntity">Entity return type</typeparam>
        /// <param name="collectionName">Collection name</param>
        /// <param name="expression">Expression filter for find a documento</param>
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
        /// <param name="expression">Expression filter for find all documents</param>
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
        /// <param name="expression">Expression filter for find all documents</param>
        /// <returns></returns>
        public Task<List<TEntity>> SelectAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).ToListAsync();
        }

        public Task<List<TEntity>> Select<TEntity, T>(string collectionName, Func<T, Expression<Func<TEntity, bool>>> expression, List<T> entities) where TEntity : class where T : class
        {
            var filters = Builders<TEntity>.Filter.And(entities?.Select(entity => Builders<TEntity>.Filter.Where(expression(entity))));

            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filters).ToListAsync();
        }

        public List<TEntity> SelectAsync<TEntity, T>(string collectionName, Func<T, Expression<Func<TEntity, bool>>> expression, List<T> entities) where TEntity : class where T : class
        {
            var filters = Builders<TEntity>.Filter.And(entities?.Select(entity => Builders<TEntity>.Filter.Where(expression(entity))));

            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filters).ToList();
        }

        public List<TEntity> SelectWithLimit<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, int limit) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).Limit(limit).ToList();
        }

        public Task<List<TEntity>> SelectWithLimitAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, int limit) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(expression).Limit(limit).ToListAsync();
        }

        public List<TEntity> SelectWithKey<TEntity>(string collectionName, string key, string value) where TEntity : class
        {
            var filter = Builders<TEntity>.Filter.Eq(key, value);

            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filter).ToList();
        }

        public List<TEntity> SelectWithKeyAsy<TEntity>(string collectionName, string key, string value) where TEntity : class
        {
            var filter = Builders<TEntity>.Filter.Eq(key, value);

            return _mongoDbClient.Collection<TEntity>(collectionName).Find(filter).ToList();
        }

        public List<TEntity> SelectAll<TEntity>(string collectionName) where TEntity : class
        {
            return _mongoDbClient.Collection<TEntity>(collectionName).Find(_ => true).ToList();
        }

        public async Task<List<TEntity>> SelectAllAsync<TEntity>(string collectionName) where TEntity : class
        {
            return await _mongoDbClient.Collection<TEntity>(collectionName).Find(_ => true).ToListAsync();
        }

        public void InsertOne<TEntity>(string collectionName, TEntity entity) where TEntity : class
        {
            _mongoDbClient.Collection<TEntity>(collectionName).InsertOne(entity);
        }

        public async Task InsertOneAsync<TEntity>(string collectionName, TEntity entity) where TEntity : class
        {
            await _mongoDbClient.Collection<TEntity>(collectionName).InsertOneAsync(entity);
        }

        public void InsertMany<TEntity>(string collectionName, List<TEntity> entities) where TEntity : class
        {
            _mongoDbClient.Collection<TEntity>(collectionName).InsertMany(entities);
        }

        public async Task InsertManyAsync<TEntity>(string collectionName, List<TEntity> entities) where TEntity : class
        {
            await _mongoDbClient.Collection<TEntity>(collectionName).InsertManyAsync(entities, new InsertManyOptions { IsOrdered = true });
        }

        public void UpdateOne<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, TEntity entity) where TEntity : class
        {
            _mongoDbClient.Collection<TEntity>(collectionName).ReplaceOne<TEntity>(expression, entity, new UpdateOptions { IsUpsert = false });
        }

        //public async Task UpdateOneAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, TEntity entity, CancellationToken cancellationToken = default) where TEntity : class
        //{
        //    var updated = Builders<TEntity>.Update.AddToSet(;
        //    var result = await _mongoDbClient.Collection<TEntity>(collectionName).UpdateOneAsync(expression, entity, new UpdateOptions { IsUpsert = true }, cancellationToken).ConfigureAwait(false);
        //    return result;
        //}

        public bool UpdateOnePush<TEntity>(string collectionName, FilterDefinition<TEntity> filter, string field, TEntity entity) where TEntity : class
        {
            var update = Builders<TEntity>.Update.Push(field, entity);
            var result = _mongoDbClient.Collection<TEntity>(collectionName).UpdateOne(filter, update);
            return result.IsAcknowledged && result.ModifiedCount > 0;
        }

        public async Task<bool> UpdateOnePushAsync<TEntity>(string collectionName, FilterDefinition<TEntity> filter, string field, TEntity entity) where TEntity : class
        {
            var update = Builders<TEntity>.Update.Push(field, entity);
            var result = await _mongoDbClient.Collection<TEntity>(collectionName).UpdateOneAsync(filter, update).ConfigureAwait(false);
            return result.IsAcknowledged && result.ModifiedCount > 0;
        }

        public bool BulkWrite<TEntity>(string collectionName, Func<TEntity, Expression<Func<TEntity, bool>>> expression, List<TEntity> entities, CancellationToken cancellationToken = default) where TEntity : class
        {
            var bulkOps = entities?.Select(entity => new ReplaceOneModel<TEntity>(expression(entity), entity) { IsUpsert = true });
            if (bulkOps != null && bulkOps.Any()) return false;
            var result = _mongoDbClient.Collection<TEntity>(collectionName).BulkWrite(bulkOps, cancellationToken: cancellationToken);
            return result.IsAcknowledged && result.Upserts.Count > 0;
        }

        public async Task<bool> BulkWriteAsync<TEntity>(string collectionName, Func<TEntity, Expression<Func<TEntity, bool>>> expression, List<TEntity> entities, CancellationToken cancellationToken = default) where TEntity : class
        {
            var bulkOps = entities?.Select(entity => new ReplaceOneModel<TEntity>(expression(entity), entity) { IsUpsert = true });
            if (bulkOps == null && !bulkOps.Any()) return false;
            var result = await _mongoDbClient.Collection<TEntity>(collectionName).BulkWriteAsync(bulkOps, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.IsAcknowledged && result.Upserts.Count > 0;
        }

        public bool DeleteOne<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            var result = _mongoDbClient.Collection<TEntity>(collectionName).DeleteOne(expression, cancellationToken);
            return result.IsAcknowledged && result.DeletedCount > 0;
        }

        public async Task<bool> DeleteOneAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            var result = await _mongoDbClient.Collection<TEntity>(collectionName).DeleteOneAsync(expression, cancellationToken).ConfigureAwait(false);
            return result.IsAcknowledged && result.DeletedCount > 0;
        }

        public bool DeleteMany<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            var result = _mongoDbClient.Collection<TEntity>(collectionName).DeleteMany(expression, cancellationToken);
            return result.IsAcknowledged && result.DeletedCount > 0;
        }

        public async Task<bool> DeleteManyAsync<TEntity>(string collectionName, Expression<Func<TEntity, bool>> expression, CancellationToken cancellationToken = default) where TEntity : class
        {
            var result = await _mongoDbClient.Collection<TEntity>(collectionName).DeleteManyAsync(expression, cancellationToken).ConfigureAwait(false);
            return result.IsAcknowledged && result.DeletedCount > 0;
        }
    }
}
