// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Azure.Services {
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.Documents.Linq;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Wraps a document query to return statements
    /// </summary>
    internal class CosmosDbFeed : IDocumentFeed {

        /// <summary>
        /// Create feed
        /// </summary>
        internal CosmosDbFeed(IDocumentQuery<dynamic> query, ILogger logger) {
            _query = query;
            _logger = logger;
        }

        /// <summary>
        /// Read from feed
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public virtual async Task<IEnumerable<dynamic>> ReadAsync(CancellationToken ct) {
            return await Retry.Do(_logger, ct, async () => {
                if (_query.HasMoreResults) {
                    return await _query.ExecuteNextAsync(ct)
                        .ConfigureAwait(false);
                }
                return Enumerable.Empty<dynamic>();
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

        private readonly IDocumentQuery<dynamic> _query;
        private readonly ILogger _logger;
    }
}
