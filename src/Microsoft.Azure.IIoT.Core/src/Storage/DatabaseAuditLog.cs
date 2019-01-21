// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Diagnostics.Models;
    using System.Threading.Tasks;
    using System.Threading;
    using System;

    /// <summary>
    /// Log entry writer based on cosmos db collection
    /// </summary>
    public sealed class DatabaseAuditLog : IAuditLog {

        /// <summary>
        /// Create writer
        /// </summary>
        /// <param name="server"></param>
        /// <param name="logger"></param>
        public DatabaseAuditLog(IDatabaseServer server, ILogger logger) {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _server = server ?? throw new ArgumentNullException(nameof(server));
        }

        /// <inheritdoc/>
        public async Task<IAuditLogWriter> OpenAsync(string log) {
            var database = await _server.OpenAsync();
            var collection = await database.OpenDocumentCollectionAsync(log);
            return new CollectionWriter(collection);
        }

        /// <summary>
        /// Collection wrapper
        /// </summary>
        private class CollectionWriter : IAuditLogWriter {

            /// <summary>
            /// Create writer
            /// </summary>
            /// <param name="collection"></param>
            public CollectionWriter(IDocumentCollection collection) {
                _collection = collection;
            }

            /// <inheritdoc/>
            public Task WriteAsync(AuditLogEntryModel entry) =>
                _collection.UpsertAsync(entry);

            private readonly IDocumentCollection _collection;
        }

        private readonly ILogger _logger;
        private readonly IDatabaseServer _server;
    }
}
