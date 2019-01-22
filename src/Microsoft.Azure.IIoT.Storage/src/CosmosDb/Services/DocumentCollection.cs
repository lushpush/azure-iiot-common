// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.Http.Exceptions;
    using Microsoft.Azure.IIoT.Http;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.Graph;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using Newtonsoft.Json.Linq;
    using Gremlin.Net.CosmosDb;
    using Gremlin.Net.CosmosDb.Structure;
    using Gremlin.Net.Process.Traversal;
    using CosmosDbCollection = Documents.DocumentCollection;

    /// <summary>
    /// Collection abstraction
    /// </summary>
    sealed class DocumentCollection : IDocumentCollection, ISqlQueryClient, IGraph {

        /// <summary>
        /// Wrapped document collection instance
        /// </summary>
        internal CosmosDbCollection Collection { get; }

        /// <summary>
        /// Create collection
        /// </summary>
        /// <param name="db"></param>
        /// <param name="collection"></param>
        /// <param name="partitioned"></param>
        /// <param name="logger"></param>
        internal DocumentCollection(DocumentDatabase db, CosmosDbCollection collection,
            bool partitioned, ILogger logger) {
            Collection = collection;
            _logger = logger;
            _partitioned = partitioned;
            _db = db;
        }

        /// <inheritdoc/>
        public IResultFeed<R> Query<T, R>(Func<IQueryable<IDocument<T>>,
            IQueryable<R>> query, int? pageSize, string partitionKey) {
            if (query == null) {
                throw new ArgumentNullException(nameof(query));
            }
            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ? null :
                new PartitionKey(partitionKey);
            var result = query(_db.Client.CreateDocumentQuery<Document>(
                UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                   new FeedOptions {
                       MaxDegreeOfParallelism = 8,
                       MaxItemCount = pageSize ?? - 1,
                       PartitionKey = pk,
                       EnableCrossPartitionQuery = pk == null
                   }).Select(d => (IDocument<T>)new DocumentWrapper<T>(d)));
            return new ResultFeed<R>(result.AsDocumentQuery(), _logger);
        }

        /// <inheritdoc/>
        public async Task<IGraphLoader> OpenBulkLoader() {

            // Clone client to set specific connection policy
            var client = new DocumentClient(_db.Client.ServiceEndpoint,
                new NetworkCredential(null, _db.Client.AuthKey).Password,
                _db.Client.ConnectionPolicy, _db.Client.ConsistencyLevel);
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            var executor = new GraphBulkExecutor(client, Collection);
            await executor.InitializeAsync();
            return new GraphLoader(executor, _logger);
        }

        /// <inheritdoc/>
        public IGremlinClient OpenGremlinClient() {
            var endpointHost = _db.Client.ServiceEndpoint.Host;
            var instanceEnd = endpointHost.IndexOf('.');
            if (instanceEnd == -1) {
                // Support local emulation
                if (!endpointHost.EqualsIgnoreCase("localhost")) {
                    throw new ArgumentException("Endpoint host invalid.");
                }
            }
            else {
                // Use the instance name but the gremlin endpoint for the server.
                endpointHost = endpointHost.Substring(0, instanceEnd) +
                    ".gremlin.cosmosdb.azure.com";
            }
            var port = _db.Client.ServiceEndpoint.Port;
            var client = new GraphClient(endpointHost + ":" + port,
                _db.DatabaseId, Collection.Id,
                new NetworkCredential(string.Empty, _db.Client.AuthKey).Password);
            return new GremlinTraversalClient(client);
        }

        /// <inheritdoc/>
        public ISqlQueryClient OpenSqlQueryClient() =>
            this;

        /// <inheritdoc/>
        public IResultFeed<IDocument<T>> Query<T>(string queryString,
            IDictionary<string, object> parameters, int? pageSize, string partitionKey) {
            if (string.IsNullOrEmpty(queryString)) {
                throw new ArgumentNullException(nameof(queryString));
            }
            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ? null :
                new PartitionKey(partitionKey);
            var query = _db.Client.CreateDocumentQuery<Document>(
                UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                new SqlQuerySpec {
                    QueryText = queryString,
                    Parameters = new SqlParameterCollection(parameters?
                        .Select(kv => new SqlParameter(kv.Key, kv.Value)) ??
                            Enumerable.Empty<SqlParameter>())
                },
                new FeedOptions {
                    MaxDegreeOfParallelism = 8,
                    MaxItemCount = pageSize ?? -1,
                    PartitionKey = pk,
                    EnableCrossPartitionQuery = pk == null
                }).Select(d => (IDocument<T>)new DocumentWrapper<T>(d));
            return new ResultFeed<IDocument<T>>(query.AsDocumentQuery(), _logger);
        }

        /// <inheritdoc/>
        public async Task<IDocument<T>> GetAsync<T>(string id, CancellationToken ct,
            string partitionKey) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ? null :
                new PartitionKey(partitionKey);
            try {
                return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                    try {
                        return new DocumentWrapper<T>(await _db.Client.ReadDocumentAsync(
                            UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id),
                            new RequestOptions { PartitionKey = pk }, ct));
                    }
                    catch (Exception ex) {
                        FilterException(ex);
                        return null;
                    }
                });
            }
            catch (ResourceNotFoundException) {
                return null;
            }
        }

        /// <inheritdoc/>
        public async Task<IDocument<T>> UpsertAsync<T>(T newItem,
            CancellationToken ct, string id, string partitionKey, string etag) {
            var ac = string.IsNullOrEmpty(etag) ? null : new AccessCondition {
                Condition = etag,
                Type = AccessConditionType.IfMatch
            };
            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ? null :
                new PartitionKey(partitionKey);
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return new DocumentWrapper<T>(await this._db.Client.UpsertDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                        DocumentCollection.GetItem(id, newItem, partitionKey),
                        new RequestOptions { AccessCondition = ac, PartitionKey = pk },
                        false, ct));
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public async Task<IDocument<T>> ReplaceAsync<T>(IDocument<T> existing,
            T newItem, CancellationToken ct) {
            if (existing == null) {
                throw new ArgumentNullException(nameof(existing));
            }
            var ac = string.IsNullOrEmpty(existing.Etag) ? null : new AccessCondition {
                Condition = existing.Etag,
                Type = AccessConditionType.IfMatch
            };
            var pk = _partitioned || string.IsNullOrEmpty(existing.PartitionKey) ? null :
                new PartitionKey(existing.PartitionKey);
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return new DocumentWrapper<T>(await this._db.Client.ReplaceDocumentAsync(
                        UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, existing.Id),
                        DocumentCollection.GetItem(existing.Id, newItem, existing.PartitionKey),
                        new RequestOptions { AccessCondition = ac, PartitionKey = pk }, ct));
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public async Task<IDocument<T>> AddAsync<T>(T newItem, CancellationToken ct,
            string id, string partitionKey) {
            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ? null :
                new PartitionKey(partitionKey);
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return new DocumentWrapper<T>(await this._db.Client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                        DocumentCollection.GetItem(id, newItem, partitionKey),
                        new RequestOptions { PartitionKey = pk }, false, ct));
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public Task DeleteAsync<T>(IDocument<T> item, CancellationToken ct) {
            if (item == null) {
                throw new ArgumentNullException(nameof(item));
            }
            return DeleteAsync(item.Id, ct, item.PartitionKey, item.Etag);
        }

        /// <inheritdoc/>
        public async Task DeleteAsync(string id, CancellationToken ct,
            string partitionKey, string etag) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            var ac = string.IsNullOrEmpty(etag) ? null : new AccessCondition {
                Condition = etag,
                Type = AccessConditionType.IfMatch
            };
            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ? null :
                new PartitionKey(partitionKey);
            await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    await _db.Client.DeleteDocumentAsync(
                        UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id),
                        new RequestOptions { AccessCondition = ac, PartitionKey = pk }, ct);
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return;
                }
            });
        }

        /// <summary>
        /// Filter exceptions
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        internal static void FilterException(Exception ex) {
            if (ex is HttpResponseException re) {
                re.StatusCode.Validate(re.Message);
            }
            else if (ex is DocumentClientException dce && dce.StatusCode.HasValue) {
                if (dce.StatusCode == (HttpStatusCode)429) {
                    throw new TemporarilyBusyException(dce.Message, dce, dce.RetryAfter);
                }
                dce.StatusCode.Value.Validate(dce.Message, dce);
            }
        }

        /// <summary>
        /// Get item
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        private static dynamic GetItem<T>(string id, T value, string partitionKey) {
            var token = JObject.FromObject(value);
            if (partitionKey != null) {
                token.AddOrUpdate(kPartitionKeyProperty, partitionKey);
            }
            if (id != null) {
                token.AddOrUpdate(kIdProperty, id);
            }
            return token;
        }

        /// <summary>
        /// Cosmos db document wrapper
        /// </summary>
        /// <typeparam name="T"></typeparam>
        internal sealed class DocumentWrapper<T> : IDocument<T> {

            /// <summary>
            /// Create document
            /// </summary>
            /// <param name="doc"></param>
            public DocumentWrapper(Document doc) {
                _doc = doc;
            }

            /// <inheritdoc/>
            public string Id => _doc.Id;

            /// <inheritdoc/>
            public T Value => (T)(dynamic)_doc;

            /// <inheritdoc/>
            public string PartitionKey => _doc.GetPropertyValue<string>(
                kPartitionKeyProperty);

            /// <inheritdoc/>
            public string Etag => _doc.ETag;

            private readonly Document _doc;
        }

        /// <summary>
        /// Gremlin traversal client
        /// </summary>
        private class GremlinTraversalClient : IGremlinTraversal {

            /// <summary>
            /// Create wrapper
            /// </summary>
            /// <param name="client"></param>
            internal GremlinTraversalClient(GraphClient client) {
                _client = client;
            }

            /// <inheritdoc/>
            public void Dispose() => _client.Dispose();

            /// <inheritdoc/>
            public ITraversal V(params (string, string)[] ids) =>
                _client.CreateTraversalSource().V(ids
                    .Select(id => (PartitionKeyIdPair)id).ToArray());

            /// <inheritdoc/>
            public ITraversal E(params string[] ids) =>
                _client.CreateTraversalSource().E(ids);

            /// <inheritdoc/>
            public IResultFeed<T> Submit<T>(string gremlin,
                int? pageSize = null, string partitionKey = null) {
                return new GremlinQueryResult<T>(_client.QueryAsync<T>(gremlin));
            }

            /// <inheritdoc/>
            public IResultFeed<T> Submit<T>(ITraversal gremlin,
                int? pageSize = null, string partitionKey = null) {
                return new GremlinQueryResult<T>(_client.QueryAsync<T>(gremlin));
            }

            /// <summary>
            /// Wraps the async query as an async result
            /// </summary>
            /// <typeparam name="T"></typeparam>
            private class GremlinQueryResult<T> : IResultFeed<T> {
                /// <inheritdoc/>
                public GremlinQueryResult(
                    Task<GraphResult<T>> query) {
                    _query = query;
                }
                /// <inheritdoc/>
                public void Dispose() => _query?.Dispose();

                /// <inheritdoc/>
                public bool HasMore() => _query != null;

                /// <inheritdoc/>
                public async Task<IEnumerable<T>> ReadAsync(
                    CancellationToken ct) {
                    var result = await _query;
                    _query = null;
                    return result;
                }

                private Task<GraphResult<T>> _query;
            }
            private readonly GraphClient _client;
        }

        /// <summary>
        /// Bulk graph loader, handles loading in batches of default 10000.
        /// </summary>
        private sealed class GraphLoader : IGraphLoader {

            /// <summary>
            /// Create loader
            /// </summary>
            /// <param name="executor"></param>
            /// <param name="logger"></param>
            /// <param name="addOnly"></param>
            /// <param name="bulkSize"></param>
            internal GraphLoader(IBulkExecutor executor, ILogger logger,
                bool addOnly = false, int bulkSize = 10000) {
                _executor = executor;
                _logger = logger;
                _bulkSize = bulkSize;
                _addOnly = addOnly;

                // Set up batch blocks
                _batcher = new BatchBlock<object>(_bulkSize,
                    new GroupingDataflowBlockOptions());
                var importer = new ActionBlock<object[]>(ProcessBatch,
                    new ExecutionDataflowBlockOptions {
                        BoundedCapacity = 1,
                        MaxDegreeOfParallelism = 1,
                        SingleProducerConstrained = true
                    });
                // Connect the output to the action handler
                _batcher.LinkTo(importer, new DataflowLinkOptions {
                    PropagateCompletion = true
                });
                // When done, cause end to be called
                _complete = _batcher.Completion
                    .ContinueWith(async t => {
                        importer.Complete();
                    // Drain
                    await importer.Completion;
                    });
                _cts = new CancellationTokenSource();
            }

            /// <inheritdoc/>
            public Task CompleteAsync(bool abort) {
                if (abort) {
                    // Cancel current import
                    _cts.Cancel();
                }
                _batcher.Complete();
                return _complete.Result;
            }

            /// <inheritdoc/>
            public Task AddVertexAsync<V>(V vertex) =>
                _batcher.SendAsync(vertex.ToVertex());

            /// <inheritdoc/>
            public Task AddEdgeAsync<V1, E, V2>(V1 from, E edge, V2 to) =>
                _batcher.SendAsync(edge.ToEdge(from, to));

            /// <summary>
            /// Imports a batch of objects
            /// </summary>
            /// <param name="obj"></param>
            /// <returns></returns>
            private Task ProcessBatch(object[] obj) {
                return Retry.WithExponentialBackoff(_logger, _cts.Token, async () => {
                    try {
                        var response = await _executor.BulkImportAsync(obj, !_addOnly,
                            true, null, null, _cts.Token);

                        // Log result
                        var wps = Math.Round(response.NumberOfDocumentsImported /
                            response.TotalTimeTaken.TotalSeconds);
                        var rps = Math.Round(response.TotalRequestUnitsConsumed /
                            response.TotalTimeTaken.TotalSeconds);
                        var rpi = response.TotalRequestUnitsConsumed /
                            response.NumberOfDocumentsImported;
                        _logger.Info($"Processed {response.NumberOfDocumentsImported} " +
                            $"elements in {response.TotalTimeTaken.TotalSeconds} sec " +
                            $"({wps} writes/s, {rps} RU/s, {rpi} RU/Element).");
                    }
                    catch (Exception ex) {
                        FilterException(ex);
                        return;
                    }
                });
            }

            private readonly Task<Task> _complete;
            private readonly CancellationTokenSource _cts;
            private readonly BatchBlock<object> _batcher;
            private readonly int _bulkSize;
            private readonly bool _addOnly;
            private readonly ILogger _logger;
            private readonly IBulkExecutor _executor;
        }

        internal const string kIdProperty = "id";
        internal const string kPartitionKeyProperty = "__pk";

        private readonly DocumentDatabase _db;
        private readonly ILogger _logger;
        private readonly bool _partitioned;
    }
}
