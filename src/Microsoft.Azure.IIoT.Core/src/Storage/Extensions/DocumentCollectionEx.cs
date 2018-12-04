// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using Microsoft.Azure.IIoT.Exceptions;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extensions on top of document collection
    /// </summary>
    public static class DocumentCollectionEx {

        /// <summary>
        /// Adds or updates an item
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="id"></param>
        /// <param name="update"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task UpsertAsync(this IDocumentCollection collection,
            string id, Func<dynamic, dynamic> update,
            CancellationToken ct = default(CancellationToken)) {
            while (true) {
                var doc = await collection.GetAsync(id, ct);
                var newdoc = update(doc);
                try {
                    if (newdoc == null) {
                        await collection.DeleteAsync(doc, null, ct);
                    }
                    else {
                        await collection.UpsertAsync(newdoc, null, ct);
                    }
                    return;
                }
                catch (ResourceOutOfDateException) {
                    // Try again...
                }
            }
        }
    }
}
