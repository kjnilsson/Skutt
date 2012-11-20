using System;

namespace Skutt.Contract
{
    public static class Postconditions
    {
        /// <summary>
        /// Ensures the string is not null or empty or whitespace
        /// </summary>
        /// <param name="value"></param>
        /// <param name="name"></param>
        public static void Enusure(string value, string name)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException("Postcondition failed: string must not be null or empty", name);
            }
        }

        /// <summary>
        /// Ensures the returned object is not null
        /// </summary>
        /// <param name="value"></param>
        /// <param name="name"></param>
        public static void Ensure(object value, string name)
        {
            if (value == null)
            {
                throw new ArgumentException("Postcondition failed: object must not be null", name);
            }
        }

        /// <summary>
        /// Ensures the expression is true.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="message"></param>
        public static void Ensure(bool expression, string message)
        {
            if (expression == false)
            {
                throw new ArgumentException(message);
            }
        }
    }
}