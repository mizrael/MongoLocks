using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Driver;
using Xunit;

namespace MongoLocks.Tests.Integration
{
    public class DummyRepositoryTests : IDisposable
    {
        private MongoClient _client;
        private IMongoDatabase _db;
        private string _dbName;

        public DummyRepositoryTests()
        {
            _dbName = $"mongoLocks_{Guid.NewGuid()}";
            _client = new MongoClient("mongodb://root:password@127.0.0.1:27017");
            _db = _client.GetDatabase(_dbName);
        }

        [Fact]
        public async Task LockAsync_should_create_and_return_locked_item_if_not_existing()
        {
            var sut = new DummyRepository(_db, TimeSpan.FromMinutes(1));

            var newItem = new Dummy(Guid.NewGuid(), "lorem ipsum");
            var item = await sut.LockAsync(newItem.Id, newItem, CancellationToken.None);
            item.Should().NotBeNull();
            item.Id.Should().Be(newItem.Id);
            item.Value.Should().Be(newItem.Value);
            item.LockId.Should().NotBeNull();
            item.LockTime.Should().NotBeNull();
        }

        [Fact]
        public async Task LockAsync_should_fail_if_item_already_locked()
        {
            var sut = new DummyRepository(_db, TimeSpan.FromMinutes(1));

            var newItem = new Dummy(Guid.NewGuid(), "lorem ipsum", null, null);
            var lockedItem = await sut.LockAsync(newItem.Id, newItem, CancellationToken.None);

            lockedItem.Should().NotBeNull();

            await Assert.ThrowsAsync<LockException>(async () => await sut.LockAsync(newItem.Id, newItem, CancellationToken.None));
        }

        [Fact]
        public async Task LockAsync_should_succeed_if_previous_lock_expired()
        {
            var sut = new DummyRepository(_db, TimeSpan.FromMilliseconds(100));

            var newItem = new Dummy(Guid.NewGuid(), "lorem ipsum", null, null);
            var expiredLockItem = await sut.LockAsync(newItem.Id, newItem, CancellationToken.None);
            expiredLockItem.Should().NotBeNull();

            await Task.Delay(1000);
            
            var lockedItem = await sut.LockAsync(newItem.Id, newItem, CancellationToken.None);
            lockedItem.Should().NotBeNull();
            lockedItem.LockId.Should().NotBeNull().And.NotBe(expiredLockItem.LockId.Value);
            lockedItem.LockTime.Should().NotBeNull().And.NotBe(expiredLockItem.LockTime.Value);
        }

        [Fact]
        public async Task ReleaseLock_should_release_lock()
        {
            var sut = new DummyRepository(_db, TimeSpan.FromMinutes(1));

            var newItem = new Dummy(Guid.NewGuid(), "lorem ipsum", null, null);
            var lockedItem = await sut.LockAsync(newItem.Id, newItem, CancellationToken.None);

            var updatedItem = lockedItem with { Value = "dolor amet"};

            await sut.ReleaseLock(updatedItem, CancellationToken.None);

            var coll = _db.GetCollection<Dummy>("dummies");
            var loadedItem = await coll.Find(e => e.Id == updatedItem.Id).FirstOrDefaultAsync();
            loadedItem.Should().NotBeNull();
            loadedItem.Value.Should().Be(updatedItem.Value);
            loadedItem.LockId.Should().BeNull();
            loadedItem.LockTime.Should().BeNull();
        }

        [Fact]
        public async Task ReleaseLock_should_fail_if_item_not_locked()
        {
            var sut = new DummyRepository(_db, TimeSpan.FromMinutes(1));

            var newItem = new Dummy(Guid.NewGuid(), "lorem ipsum", null, null);
        
            await Assert.ThrowsAsync<LockException>(async () => await sut.ReleaseLock(newItem, CancellationToken.None));
        }

        public void Dispose()
        {
            _client?.DropDatabase(_dbName);
        }
    }
}
