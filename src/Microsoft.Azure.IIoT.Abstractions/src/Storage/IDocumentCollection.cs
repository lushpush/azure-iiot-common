// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using Microsoft.Azure.IIoT.Exceptions;
    using System;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a collection of documents
    /// </summary>
    public interface IDocumentCollection {

        /// <summary>
        /// Add new item
        /// </summary>
        /// <param name="newItem"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<dynamic> AddAsync(dynamic newItem,
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
        /// Replace item
        /// </summary>
        /// <param name="itemOrId"></param>
        /// <param name="eTag"></param>
        /// <param name="newItem"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<dynamic> ReplaceAsync(dynamic itemOrId, dynamic newItem,
            CancellationToken ct = default(CancellationToken),
            string eTag = null);

        /// <summary>
        /// Adds or updates an item.  If etag is not provided,
        /// tries to read etag from the dynamic object based
        /// on implementation.
        /// </summary>
        /// <exception cref="ResourceOutOfDateException"/>
        /// <param name="newItem"></param>
        /// <param name="ct"></param>
        /// <param name="etag"></param>
        /// <returns></returns>
        Task<dynamic> UpsertAsync(dynamic newItem,
            CancellationToken ct = default(CancellationToken),
            string etag = null);

        /// <summary>
        /// Query items
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        IDocumentFeed Query<T>(Expression<Func<T, bool>> predicate,
            int? pageSize = null);

        /// <summary>
        /// Removes the item using the passed in id or the item
        /// object itself.
        /// </summary>
        /// <exception cref="ResourceOutOfDateException"/>
        /// <param name="itemOrId"></param>
        /// <param name="ct"></param>
        /// <param name="eTag"></param>
        /// <returns></returns>
        Task DeleteAsync(dynamic itemOrId,
            CancellationToken ct = default(CancellationToken),
            string eTag = null);
    }
}
