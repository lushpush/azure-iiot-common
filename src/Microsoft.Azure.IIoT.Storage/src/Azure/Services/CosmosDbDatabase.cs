// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Azure.Services {
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
    using Microsoft.Azure.IIoT.Diagnostics;
    using System.Text;

    /// <summary>
    /// Provides document db database interface.
    /// </summary>
    internal class CosmosDbDatabase : IDocumentDatabase {

        /// <summary>
        /// Database id
        /// </summary>
        internal string DatabaseId { get; }

        /// <summary>
        /// Client
        /// </summary>
        internal DocumentClient Client { get; }

        /// <summary>
        /// Creates server
        /// </summary>
        /// <param name="client"></param>
        /// <param name="id"></param>
        /// <param name="logger"></param>
        public CosmosDbDatabase(DocumentClient client, string id, ILogger logger) {
            _logger = logger;
            Client = client;
            _collections = new ConcurrentDictionary<string, CosmosDbCollection>();
            DatabaseId = id;
        }

        /// <summary>
        /// Open collection as graph collection.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<IDocumentCollection> OpenCollectionAsync(string id) {
            if (string.IsNullOrEmpty(id)) {
                id = "default";
            }
            if (!_collections.TryGetValue(id, out var collection)) {
                var coll = await EnsureCollectionExists(id);
                collection = _collections.GetOrAdd(id, k =>
                    new CosmosDbCollection(this, coll, _logger));
            }
            return collection;
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
                var response = await Client.ReadDocumentCollectionFeedAsync(
                    UriFactory.CreateDatabaseUri(DatabaseId),
                    new FeedOptions {
                        RequestContinuation = continuation
                    });
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
            await Client.DeleteDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(DatabaseId, id));
            _collections.TryRemove(id, out var collection);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose() {
            _collections.Clear();
            Client.Dispose();
        }

        /// <summary>
        /// Ensures collection exists
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private async Task<DocumentCollection> EnsureCollectionExists(string id) {
            var database = await Client.CreateDatabaseIfNotExistsAsync(
                new Database {
                    Id = DatabaseId
                }
            );

            var collectionDefinition = new DocumentCollection {
                Id = id,
                DefaultTimeToLive = -1, // Infinite
                IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) {
                    Precision = -1
                })
            };

            var throughput = 10000;
            var collection = await Client.CreateDocumentCollectionIfNotExistsAsync(
                 UriFactory.CreateDatabaseUri(DatabaseId),
                 collectionDefinition,
                 new RequestOptions {
                     OfferThroughput = throughput
                 }
            );

            // await CreateSprocIfNotExists(id, kBulkUpdateSprocName);
            // await CreateSprocIfNotExists(id, kBulkDeleteSprocName);
            return collection.Resource;
        }

        /// <summary>
        /// Create stored procedures
        /// </summary>
        /// <param name="collectionId"></param>
        /// <param name="sprocName"></param>
        /// <returns></returns>
        private async Task CreateSprocIfNotExists(string collectionId, string sprocName) {
            var assembly = GetType().Assembly;
#if FALSE
            try {
                var sprocUri = UriFactory.CreateStoredProcedureUri(
                    DatabaseId, collectionId, sprocName);
                await _client.DeleteStoredProcedureAsync(sprocUri);
            }
            catch (DocumentClientException) {}
#endif
            var resource = $"{assembly.GetName().Name}.Azure.Script.{sprocName}.js";
            using (var stream = assembly.GetManifestResourceStream(resource)) {
                if (stream == null) {
                    throw new FileNotFoundException(resource + " not found");
                }
                var sproc = new StoredProcedure {
                    Id = sprocName,
                    Body = stream.ReadAsString(Encoding.UTF8)
                };
                try {
                    var sprocUri = UriFactory.CreateStoredProcedureUri(
                        DatabaseId, collectionId, sprocName);
                    await Client.ReadStoredProcedureAsync(sprocUri);
                    return;
                }
                catch (DocumentClientException de) {
                    if (de.StatusCode != HttpStatusCode.NotFound) {
                        throw;
                    }
                }
                await Client.CreateStoredProcedureAsync(
                    UriFactory.CreateDocumentCollectionUri(DatabaseId,
                    collectionId), sproc);
            }
        }

        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, CosmosDbCollection> _collections;
        internal const string kBulkUpdateSprocName = "bulkUpdate";
        internal const string kBulkDeleteSprocName = "bulkDelete";
    }
}
