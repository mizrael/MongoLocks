using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;

namespace MongoLocks
{
    public class DummyRepository
    {
        private readonly IMongoDatabase _db;
        private readonly IMongoCollection<Dummy> _collection;
        private readonly TimeSpan _lockMaxDuration = TimeSpan.FromMinutes(1);
        
        private static readonly IBsonSerializer<Guid> guidSerializer = new GuidSerializer(GuidRepresentation.Standard);
        private static readonly IBsonSerializer nullableGuidSerializer = new NullableSerializer<Guid>(guidSerializer);
        
        private const string CollectionName = "dummies";

        static DummyRepository()
        {
            BsonDefaults.GuidRepresentationMode = GuidRepresentationMode.V3;
            
            if (!BsonClassMap.IsClassMapRegistered(typeof(Dummy)))
                BsonClassMap.RegisterClassMap<Dummy>(mapper =>
                {
                    mapper.MapIdField(c => c.Id).SetSerializer(guidSerializer);
                    mapper.MapProperty(c => c.Value);
                    mapper.MapProperty(c => c.LockId).SetSerializer(nullableGuidSerializer)
                                                     .SetDefaultValue(() => null);
                    mapper.MapProperty(c => c.LockTime).SetDefaultValue(() => null);
                    mapper.MapCreator(s => new Dummy(s.Id, s.Value, s.LockId, s.LockTime));
                });
        }

        public DummyRepository(IMongoDatabase client, TimeSpan lockMaxDuration)
        {
            _db = client ?? throw new ArgumentNullException(nameof(client));
            _collection = _db.GetCollection<Dummy>(CollectionName);
            _lockMaxDuration = lockMaxDuration;
        }

        public async Task<Dummy> LockAsync(Guid id, Dummy newEntity, CancellationToken cancellationToken = default)
        {
            var filter = Builders<Dummy>.Filter.And(
                Builders<Dummy>.Filter.Eq(e => e.Id, id),
                Builders<Dummy>.Filter.Or(
                    Builders<Dummy>.Filter.Eq(e => e.LockId, null),
                    Builders<Dummy>.Filter.Lt(e => e.LockTime, DateTime.UtcNow - _lockMaxDuration)
                )
            );
            var update = Builders<Dummy>.Update
                .Set(e => e.LockId, Guid.NewGuid())
                .Set(e => e.LockTime, DateTime.UtcNow);
            
            if (newEntity is not null)
            {
                update = update.SetOnInsert(e => e.Id, newEntity.Id)
                               .SetOnInsert(e => e.Value, newEntity.Value);
            }
                
            var options = new FindOneAndUpdateOptions<Dummy>()
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After
            };

            try
            {
                var entity = await _collection.FindOneAndUpdateAsync(filter, update, options, cancellationToken)
                                                .ConfigureAwait(false);
                return entity;
            }
            catch (MongoCommandException e) when(e.Code == 11000 && e.CodeName == "DuplicateKey")
            {
                throw new LockException($"item '{id}' is already locked");
            }
        }

        public async Task ReleaseLock(Dummy item, CancellationToken cancellationToken = default)
        {
            if (item == null) 
                throw new ArgumentNullException(nameof(item));
            
            var filter = Builders<Dummy>.Filter.And(
                Builders<Dummy>.Filter.Eq(e => e.Id, item.Id),
                Builders<Dummy>.Filter.Eq(e => e.LockId, item.LockId)
            );

            var update = Builders<Dummy>.Update
                .Set(e => e.Value, item.Value)
                .Set(e => e.LockId, null)
                .Set(e => e.LockTime, null);
            
            var options = new UpdateOptions()
            {
                IsUpsert = false
            };

            var result = await _collection.UpdateOneAsync(filter, update, options, cancellationToken)
                                        .ConfigureAwait(false);
            if (result is null || result.ModifiedCount != 1)
                throw new LockException($"unable to release lock on item '{item.Id}'");
        }
    }
}
