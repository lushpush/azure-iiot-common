// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using System.Threading;
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using System.Linq;
    using System;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.IIoT.Utils;

    /// <summary>
    /// Wraps a document query to return statements
    /// </summary>
    internal class DocumentFeed<I, O> : IDocumentFeed<O> {

        /// <summary>
        /// Create feed
        /// </summary>
        internal DocumentFeed(IDocumentQuery<I> query,
            Func<IEnumerable<dynamic>, IEnumerable<O>> convert) {
            _query = query;
            _convert = convert;
        }

        /// <summary>
        /// Read from feed
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public virtual async Task<IEnumerable<O>> ReadAsync(CancellationToken ct) {
            return await Retry.Do(ct, async () => {
                if (_query.HasMoreResults) {
                    var result = await _query.ExecuteNextAsync(ct)
                        .ConfigureAwait(false);
                    return _convert(result);
                }
                return Enumerable.Empty<O>();
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        public bool HasMore() => _query.HasMoreResults;

        /// <summary>
        /// Dispose query
        /// </summary>
        public void Dispose() {
            _query.Dispose();
        }

        private readonly IDocumentQuery<I> _query;
        private readonly Func<IEnumerable<dynamic>, IEnumerable<O>> _convert;
    }
}
