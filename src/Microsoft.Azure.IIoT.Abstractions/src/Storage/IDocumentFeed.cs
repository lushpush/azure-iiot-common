// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Base feed interfce
    /// </summary>
    public interface IDocumentFeed : IDisposable {
        /// <summary>
        /// Returns whether there is more data in the feed
        /// </summary>
        /// <returns></returns>
        bool HasMore();
    }

    /// <summary>
    /// Typed feed reader
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IDocumentFeed<T> : IDocumentFeed {

        /// <summary>
        /// Read results from feed
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<T>> ReadAsync(CancellationToken ct);
    }
}
