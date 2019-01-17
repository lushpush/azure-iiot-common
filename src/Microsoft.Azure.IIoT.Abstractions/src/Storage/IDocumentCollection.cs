// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using Microsoft.Azure.IIoT.Exceptions;
    using System;
    using System.Linq;
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
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        Task<IDocument<T>> AddAsync<T>(T newItem,
            CancellationToken ct = default(CancellationToken),
            string id = null, string partitionKey = null);

        /// <summary>
        /// Gets an item.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ct"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        Task<IDocument<T>> GetAsync<T>(string id,
            CancellationToken ct = default(CancellationToken),
            string partitionKey = null);

        /// <summary>
        /// Replace item
        /// </summary>
        /// <param name="existing"></param>
        /// <param name="value"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<IDocument<T>> ReplaceAsync<T>(IDocument<T> existing,
            T value, CancellationToken ct = default(CancellationToken));

        /// <summary>
        /// Adds or updates an item.
        /// </summary>
        /// <exception cref="ResourceOutOfDateException"/>
        /// <param name="newItem"></param>
        /// <param name="ct"></param>
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        /// <param name="etag"></param>
        /// <returns></returns>
        Task<IDocument<T>> UpsertAsync<T>(T newItem,
            CancellationToken ct = default(CancellationToken),
            string id = null, string partitionKey = null,
            string etag = null);

        /// <summary>
        /// Query items using expressions
        /// </summary>
        /// <param name="query"></param>
        /// <param name="partitionKey"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        IResultFeed<R> Query<T, R>(
            Func<IQueryable<IDocument<T>>, IQueryable<R>> query,
            int? pageSize = null, string partitionKey = null);

        /// <summary>
        /// Query using sql. If not supported will throw.
        /// </summary>
        /// <exception cref="NotSupportedException"/>
        /// <returns></returns>
        ISqlQueryClient OpenSqlQueryClient();

        /// <summary>
        /// Removes the item.
        /// </summary>
        /// <exception cref="ResourceOutOfDateException"/>
        /// <param name="item"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeleteAsync<T>(IDocument<T> item,
            CancellationToken ct = default(CancellationToken));

        /// <summary>
        /// Delete an item by id.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ct"></param>
        /// <param name="partitionKey"></param>
        /// <param name="etag"></param>
        /// <returns></returns>
        Task DeleteAsync(string id,
            CancellationToken ct = default(CancellationToken),
            string partitionKey = null, string etag = null);
    }
}
