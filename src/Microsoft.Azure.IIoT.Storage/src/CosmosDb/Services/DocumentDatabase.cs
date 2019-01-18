// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;
    using System;
    using System.Text;
    using System.Net;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    /// <summary>
    /// Provides document db database interface.
    /// </summary>
    sealed class DocumentDatabase : IDatabase {

        /// <summary>
        /// Database id
        /// </summary>
        internal string DatabaseId { get; }

        /// <summary>
        /// Client
        /// </summary>
        internal DocumentClient Client { get; }

        /// <summary>
        /// Creates database
        /// </summary>
        /// <param name="client"></param>
        /// <param name="id"></param>
        /// <param name="logger"></param>
        public DocumentDatabase(DocumentClient client, string id, ILogger logger) {
            _logger = logger;
            Client = client;
            DatabaseId = id;
            _collections = new ConcurrentDictionary<string, QueryableCollection>();
        }

        /// <inheritdoc/>
        public async Task<IDocumentCollection> OpenDocumentCollectionAsync(string id,
            bool partitioned) => await OpenOrCreateCollectionAsync(id, partitioned);

        /// <inheritdoc/>
        public async Task<IGraph> OpenGraphCollectionAsync(string id,
            bool partitioned) => await OpenOrCreateCollectionAsync(id, partitioned);

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public async Task DeleteCollectionAsync(string id) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            await Client.DeleteDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(DatabaseId, id));
            _collections.TryRemove(id, out var collection);
        }

        /// <inheritdoc/>
        public void Dispose() {
            _collections.Clear();
            Client.Dispose();
        }

        /// <summary>
        /// Create or Open collection
        /// </summary>
        /// <param name="id"></param>
        /// <param name="partitioned"></param>
        /// <returns></returns>
        private async Task<QueryableCollection> OpenOrCreateCollectionAsync(
            string id, bool partitioned) {
            if (string.IsNullOrEmpty(id)) {
                id = "default";
            }
            if (!_collections.TryGetValue(id, out var collection)) {
                var coll = await EnsureCollectionExists(id, partitioned);
                collection = _collections.GetOrAdd(id, k =>
                    new QueryableCollection(this, coll, partitioned, _logger));
            }
            return collection;
        }

        /// <summary>
        /// Ensures collection exists
        /// </summary>
        /// <param name="id"></param>
        /// <param name="partitioned"></param>
        /// <returns></returns>
        private async Task<Documents.DocumentCollection> EnsureCollectionExists(string id,
            bool partitioned) {
            var database = await Client.CreateDatabaseIfNotExistsAsync(
                new Documents.Database {
                    Id = DatabaseId
                }
            );

            var collectionDefinition = new Documents.DocumentCollection {
                Id = id,
                DefaultTimeToLive = -1, // Infinite
                IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) {
                    Precision = -1
                })
            };

            if (partitioned) {
                collectionDefinition.PartitionKey.Paths.Add(
                    "/" + QueryableCollection.kPartitionKeyProperty);
            }

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
        private readonly ConcurrentDictionary<string, QueryableCollection> _collections;
    }
}
