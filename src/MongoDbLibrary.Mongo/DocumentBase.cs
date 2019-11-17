using MongoDB.Bson.Serialization.Attributes;
using System.Runtime.Serialization;

namespace MongoDbLibrary.Mongo
{
    [DataContract]
    public abstract class DocumentBase<TKey>
    {
        [BsonId]
        [DataMember(Name = "_id")]
        public virtual TKey Id { get; set; }
    }
}