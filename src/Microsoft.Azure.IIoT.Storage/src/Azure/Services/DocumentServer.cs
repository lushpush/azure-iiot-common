// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;
    using System;
    using System.Net;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    /// <summary>
    /// Provides document db and graph functionality for storage interfaces.
    /// </summary>
    public class DocumentServer : IDocumentServer {

        /// <summary>
        /// Creates server with optional change processor implementation
        /// </summary>
        /// <param name="connectionString"></param>
        public DocumentServer(ConnectionString connectionString) {
            _connectionString = connectionString ??
                throw new ArgumentNullException(nameof(connectionString));
            _client = new DocumentClient(
                new Uri(_connectionString.AccountEndpoint),
                _connectionString.AccountKey,
                new ConnectionPolicy {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                }
            );
            _collections = new ConcurrentDictionary<string, DocumentSet>();
        }

        /// <summary>
        /// Open collection as graph collection.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<IDocumentCollection> OpenGraphCollectionAsync(string id) {
            if (string.IsNullOrEmpty(id)) {
                id = "default";
            }
            if (!_collections.TryGetValue(id, out var collection)) {
                var coll = await EnsureCollectionExists(id).ConfigureAwait(false);
                collection = _collections.GetOrAdd(id, k =>
                    new DocumentSet(this, _client, _connectionString.Database, coll));
            }
            return new GraphCollection(collection);
        }

        /// <summary>
        /// Read list of collections
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task<IEnumerable<string>> ListCollectionsAsync(CancellationToken ct) {
            var continuation = string.Empty;
            var result = new List<string>();
            do {
                var response = await _client.ReadDocumentCollectionFeedAsync(
                    UriFactory.CreateDatabaseUri(_connectionString.Database),
                    new FeedOptions {
                        RequestContinuation = continuation
                    }).ConfigureAwait(false);
                continuation = response.ResponseContinuation;
                result.AddRange(response.Select(c => c.Id));
            }
            while (!string.IsNullOrEmpty(continuation));
            return result;
        }

        /// <summary>
        /// Delete collection
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task DeleteCollectionAsync(string id) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            await _client.DeleteDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(_connectionString.Database, id))
                .ConfigureAwait(false);
            if (_collections.TryRemove(id, out var collection)) {
                collection.Dispose();
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose() {
            foreach (var collection in _collections) {
                collection.Value.Dispose();
            }
            _collections.Clear();
            _client.Dispose();
        }

        /// <summary>
        /// Ensures collection exists
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private async Task<DocumentCollection> EnsureCollectionExists(string id) {
            var database = await _client.CreateDatabaseIfNotExistsAsync(
                new Database {
                    Id = _connectionString.Database
                }
            ).ConfigureAwait(false);

            var collectionDefinition = new DocumentCollection {
                Id = id,
                DefaultTimeToLive = -1, // Infinite
                IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) {
                    Precision = -1
                })
            };

            var throughput = 10000;

            var collection = await _client.CreateDocumentCollectionIfNotExistsAsync(
                 UriFactory.CreateDatabaseUri(_connectionString.Database),
                 collectionDefinition,
                 new RequestOptions {
                     OfferThroughput = throughput
                 }
            ).ConfigureAwait(false);

            await CreateSprocIfNotExists(id, BulkUpdateSprocName).ConfigureAwait(false);
            await CreateSprocIfNotExists(id, BulkDeleteSprocName).ConfigureAwait(false);
            return collection.Resource;
        }

        /// <summary>
        /// Create stored procedures
        /// </summary>
        /// <param name="collectionId"></param>
        /// <param name="sprocName"></param>
        /// <returns></returns>
        private async Task CreateSprocIfNotExists(string collectionId, string sprocName) {
            var assembly = this.GetType().Assembly;

#if FALSE
            try {
                var sprocUri = UriFactory.CreateStoredProcedureUri(
                    _connectionString.Database, collectionId, sprocName);
                await _client.DeleteStoredProcedureAsync(sprocUri).ConfigureAwait(false);
            }
            catch (DocumentClientException) {}
#endif

            var resource = $"{assembly.GetName().Name}.Script.{sprocName}.js";
            using (var stream = assembly.GetManifestResourceStream(resource)) {
                if (stream == null) {
                    throw new FileNotFoundException(resource + " not found");
                }
                var sproc = new StoredProcedure {
                    Id = sprocName,
                    Body = await stream.ToStringAsync().ConfigureAwait(false)
                };
                try {
                    var sprocUri = UriFactory.CreateStoredProcedureUri(
                        _connectionString.Database, collectionId, sprocName);
                    await _client.ReadStoredProcedureAsync(sprocUri).ConfigureAwait(false);
                    return;
                }
                catch (DocumentClientException de) {
                    if (de.StatusCode != HttpStatusCode.NotFound) {
                        throw;
                    }
                }
                await _client.CreateStoredProcedureAsync(
                    UriFactory.CreateDocumentCollectionUri(_connectionString.Database,
                    collectionId), sproc).ConfigureAwait(false);
            }
        }


        private readonly ConnectionString _connectionString;
        private readonly DocumentClient _client;
        private readonly ConcurrentDictionary<string, DocumentSet> _collections;

        internal const string BulkUpdateSprocName = "bulkUpdate";
        internal const string BulkDeleteSprocName = "bulkDelete";
    }
}
