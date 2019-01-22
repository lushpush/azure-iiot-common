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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using GremlinClient = Gremlin.Net.Driver.GremlinClient;
    using GremlinServer = Gremlin.Net.Driver.GremlinServer;

    /// <summary>
    /// Collection abstraction
    /// </summary>
    sealed class QueryableCollection : IDocumentCollection, ISqlQueryClient, IGraph {

        /// <summary>
        /// Wrapped document collection instance
        /// </summary>
        internal DocumentCollection Collection { get; }

        /// <summary>
        /// Create collection
        /// </summary>
        /// <param name="db"></param>
        /// <param name="collection"></param>
        /// <param name="partitioned"></param>
        /// <param name="logger"></param>
        internal QueryableCollection(DocumentDatabase db, DocumentCollection collection,
            bool partitioned, ILogger logger) {
            Collection = collection;
            _logger = logger;
            _partitioned = partitioned;
            _db = db;
            _server = new Lazy<GremlinServer>(CreateGremlinServer);
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
            return new DocumentFeed<R>(result.AsDocumentQuery(), _logger);
        }

        /// <inheritdoc/>
        public IGremlinClient OpenGremlinClient() =>
            new GremlinClientWrapper(_server.Value);

        /// <inheritdoc/>
        public IBulkLoader GetBulkLoader() {
            throw new NotImplementedException();
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
            return new DocumentFeed<IDocument<T>>(query.AsDocumentQuery(), _logger);
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
                    return new DocumentWrapper<T>(await _db.Client.UpsertDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                        GetItem(id, newItem, partitionKey),
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
                    return new DocumentWrapper<T>(await _db.Client.ReplaceDocumentAsync(
                        UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, existing.Id),
                        GetItem(existing.Id, newItem, existing.PartitionKey),
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
                    return new DocumentWrapper<T>(await _db.Client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                        GetItem(id, newItem, partitionKey), new RequestOptions { PartitionKey = pk },
                        false, ct));
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
        /// Create gremlin server from document client
        /// </summary>
        /// <returns></returns>
        private GremlinServer CreateGremlinServer() {
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
            return new GremlinServer(endpointHost, port, true,
                "/dbs/" + _db.DatabaseId + "/colls/" + Collection.Id,
                new NetworkCredential(string.Empty, _db.Client.AuthKey).Password);
        }

        /// <summary>
        /// Gremlin client
        /// </summary>
        internal class GremlinClientWrapper : IGremlinClient {

            /// <summary>
            /// Create wrapper
            /// </summary>
            /// <param name="server"></param>
            public GremlinClientWrapper(GremlinServer server) {
                _wrapped = new GremlinClient(server, null, null, null);
            }

            /// <inheritdoc/>
            public void Dispose() => _wrapped.Dispose();

            /// <inheritdoc/>
            public IResultFeed<T> Submit<T>(string gremlin,
                int? pageSize = null, string partitionKey = null) {
                return new GremlinQueryResult<T>(_wrapped.SubmitAsync<T>(
                    Gremlin.Net.Driver.Messages.RequestMessage
                        .Build(gremlin)
                        .Create()));
            }

            /// <summary>
            /// Wraps the async query as an async result
            /// </summary>
            /// <typeparam name="T"></typeparam>
            private class GremlinQueryResult<T> : IResultFeed<T> {
                /// <inheritdoc/>
                public GremlinQueryResult(
                    Task<Gremlin.Net.Driver.ResultSet<T>> query) {
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

                private Task<Gremlin.Net.Driver.ResultSet<T>> _query;
            }
            private readonly GremlinClient _wrapped;
        }

        internal const string kIdProperty = "id";
        internal const string kPartitionKeyProperty = "__pk";

        private readonly DocumentDatabase _db;
        private readonly ILogger _logger;
        private readonly bool _partitioned;
        private readonly Lazy<GremlinServer> _server;
    }
}
