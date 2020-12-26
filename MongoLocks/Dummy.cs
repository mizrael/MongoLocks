using System;

namespace MongoLocks
{
    public record Dummy(Guid Id, string Value, Guid? LockId = null, DateTime? LockTime = null);
}