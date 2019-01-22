
namespace Microsoft.Azure.IIoT.Storage.Annotations {
    using System;

    /// <summary>
    /// Declarative label name of a schema-bound graph element
    /// </summary>
    /// <seealso cref="System.Attribute"/>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property,
        AllowMultiple = false, Inherited = false)]
    public sealed class TypeNameAttribute : Attribute {
        /// <summary>
        /// Gets the name.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Initializes a new instance of the
        /// <see cref="TypeNameAttribute"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <exception cref="ArgumentNullException">name</exception>
        public TypeNameAttribute(string name) {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }
    }
}
