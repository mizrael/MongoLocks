using System;

namespace MongoLocks
{
    public class LockException : Exception
    {
        public LockException(string msg) : base(msg)
        {
        }
    }
}