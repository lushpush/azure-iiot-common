// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a document database
    /// </summary>
    public interface IDocumentDatabase {

        /// <summary>
        /// Opens or creates a collection.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Task<IDocumentCollection> OpenCollectionAsync(
            string id = null);

        /// <summary>
        /// List all stores in the database
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<IEnumerable<string>> ListCollectionsAsync(
            CancellationToken ct = default(CancellationToken));

        /// <summary>
        /// Delete collection
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Task DeleteCollectionAsync(string id = null);
    }
}
