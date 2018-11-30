// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents collection items
    /// </summary>
    public interface IDocumentCollection : IDisposable {

        /// <summary>
        /// Adds an item
        /// </summary>
        /// <param name="item"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task UpsertAsync<T>(T item, CancellationToken ct);

        /// <summary>
        /// Removes the item.
        /// </summary>
        /// <param name="item"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeleteAsync<T>(T item, CancellationToken ct);

        /// <summary>
        /// Get all items
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<IDocumentFeed<T>> GetAllAsync<T>(CancellationToken ct);

        /// <summary>
        /// Removes all items
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DropAllAsync(CancellationToken ct);
    }
}
