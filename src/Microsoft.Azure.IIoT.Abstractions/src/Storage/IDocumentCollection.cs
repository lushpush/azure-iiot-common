// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using System;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a collection of documents
    /// </summary>
    public interface IDocumentCollection {

        /// <summary>
        /// Adds an item
        /// </summary>
        /// <param name="item"></param>
        /// <param name="etag"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<dynamic> UpsertAsync(dynamic item, string etag = null,
            CancellationToken ct = default(CancellationToken));

        /// <summary>
        /// Gets the item.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<dynamic> GetAsync(string id,
            CancellationToken ct = default(CancellationToken));

        /// <summary>
        /// Query items
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        IDocumentFeed QueryAsync(
            Expression<Func<dynamic, bool>> query);

        /// <summary>
        /// Removes the item.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="etag"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeleteAsync(string id, string etag = null,
            CancellationToken ct = default(CancellationToken));
    }
}
