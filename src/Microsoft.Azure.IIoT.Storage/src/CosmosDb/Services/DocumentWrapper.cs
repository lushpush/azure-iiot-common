// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.Documents;

    /// <summary>
    /// Cosmos db document wrapper
    /// </summary>
    /// <typeparam name="T"></typeparam>
    sealed class DocumentWrapper<T> : IDocument<T> {

        /// <summary>
        /// Create document
        /// </summary>
        /// <param name="doc"></param>
        public DocumentWrapper(Document doc) {
            _doc = doc;
        }

        /// <inheritdoc/>
        public string Id => _doc.Id;

        /// <inheritdoc/>
        public T Value => (T)(dynamic)_doc;

        /// <inheritdoc/>
        public string PartitionKey => _doc.GetPropertyValue<string>(
            DatabaseCollection.kPartitionKeyProperty);

        /// <inheritdoc/>
        public string Etag => _doc.ETag;

        private readonly Document _doc;
    }
}
