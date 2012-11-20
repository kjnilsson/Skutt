using System;
using System.IO;

namespace Skutt.Contract
{
    /// <summary>
    /// A poor mans code contracts
    /// Contains convenience methods for argument validation
    /// </summary>
    public static class Preconditions
    {
        /// <summary>
        /// Ensures the string is not null or empty or whitespace
        /// </summary>
        /// <param name="value"></param>
        /// <param name="name"></param>
        public static void Require(string value, string name)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException("String must not be null or empty", name);
            }
        }

        /// <summary>
        /// Ensures the passed object is not null or default.
        /// </summary>
        /// <param name="value">The object to validate.</param>
        /// <param name="name">The name of the parameter. This will be used in the exception message.</param>
        public static void Require<T>(T value, string name)
        {
            var defaultValue = default(T);

            if (value == null || value.Equals(defaultValue))
            {
                throw new ArgumentException(string.Format("{0} must not be null or default", typeof(T).Name), name);
            }
        }

        /// <summary>
        /// Ensures the expression is true.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="message"></param>
        public static void Require(bool expression, string message)
        {
            if(expression == false)
            {
                throw new ArgumentException(message);
            }
        }

        public static void RequireDirectory(string path)
        {
            if(Directory.Exists(path) == false)
            {
                throw new PreconditionFailedException(string.Format("Required path: {0} does not exist", path));
            }
        }
    }
}
