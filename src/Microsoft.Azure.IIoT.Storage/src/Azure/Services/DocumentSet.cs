// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using System;
    using System.Net;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.IIoT.Utils;
    using Newtonsoft.Json.Linq;
    using Microsoft.Azure.Graphs;
    using Microsoft.Azure.Documents.Linq;
    using System.Collections.Generic;
    using Microsoft.Azure.IIoT.Utils;

    /// <summary>
    /// Document collection wrapper - called Document"Set" to not conflict with
    /// DocumentCollection structure in SDK.
    /// </summary>
    internal class DocumentSet : IDocumentCollection, IDisposable {

        /// <summary>
        /// Client
        /// </summary>
        public DocumentClient Client { get; }

        /// <summary>
        /// Database id
        /// </summary>
        public string DatabaseId { get; }

        /// <summary>
        /// Collection uri
        /// </summary>
        public Uri CollectionUri { get; }

        /// <summary>
        /// Collection
        /// </summary>
        public DocumentCollection Collection { get; }

        /// <summary>
        /// Creates document collection wrapper
        /// </summary>
        /// <param name="client"></param>
        /// <param name="databaseId"></param>
        /// <param name="collection"></param>
        internal DocumentSet(DocumentClient client, string databaseId,
            DocumentCollection collection) {
            Client = client ??
                throw new ArgumentNullException(nameof(client));
            DatabaseId = databaseId ??
                throw new ArgumentNullException(nameof(databaseId));
            Collection = collection ??
                throw new ArgumentNullException(nameof(collection));
            CollectionUri =
                UriFactory.CreateDocumentCollectionUri(databaseId, Collection.Id);
        }

        /// <summary>
        /// Create query
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public IQueryable<T> CreateQuery<T>(string partitionKey, int batchSize = -1) {
            return Client.CreateDocumentQuery<T>(CollectionUri,
                CreateFeedOptions(partitionKey, batchSize));
        }

        /// <summary>
        /// Create sql query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="partitionKey"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        internal IDocumentQuery<T> CreateSqlQuery<T>(SqlQuerySpec query,
            string partitionKey, int batchSize = -1) {
            return Client.CreateDocumentQuery<T>(CollectionUri, query,
                CreateFeedOptions(partitionKey, batchSize)).AsDocumentQuery();
        }

        /// <summary>
        /// Update document
        /// </summary>
        /// <param name="id"></param>
        /// <param name="update"></param>
        /// <param name="partitionKey"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task UpdateAsync(string id, Func<JObject, JObject> update,
            string partitionKey, CancellationToken ct) {
            var uri = UriFactory.CreateDocumentUri(DatabaseId, Collection.Id, id);
            await Retry.Do(ct, async () => {
                JObject item = null;
                AccessCondition condition = null;
                var requestOptions = CreateRequestOptions(partitionKey);
                try {
                    var reply = await Client.ReadDocumentAsync(uri, requestOptions)
                        .ConfigureAwait(false);
                    item = JObject.FromObject((dynamic)reply?.Resource);
                    condition = new AccessCondition {
                        Condition = reply.Resource.ETag,
                        Type = AccessConditionType.IfMatch
                    };
                    ResponseUtils.CheckResponse(reply.StatusCode, HttpStatusCode.OK);
                }
                catch (DocumentClientException dce)
                    when (dce.StatusCode == HttpStatusCode.NotFound) {
                    // Not found - continue by inserting
                }

                var newItem = update(item);
                if (newItem == null) {
                    return;
                }
                if (requestOptions != null) {
                    requestOptions.AccessCondition = condition;
                }
                else {
                    requestOptions = new RequestOptions { AccessCondition = condition };
                }
                var response = await Client.UpsertDocumentAsync(
                    CollectionUri, newItem, requestOptions).ConfigureAwait(false);
                ResponseUtils.CheckResponse(response.StatusCode,
                    HttpStatusCode.OK, HttpStatusCode.Created);
            }, ResponseUtils.ContinueWithPrecondition, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Delete item
        /// </summary>
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task DeleteAsync(string id, string partitionKey, CancellationToken ct) {
            var uri = UriFactory.CreateDocumentUri(DatabaseId, Collection.Id, id);
            await Retry.Do(ct, async () => {
                Resource resource = null;
                var requestOptions = CreateRequestOptions(partitionKey);
                try {
                    var reply = await Client.ReadDocumentAsync(uri, requestOptions)
                        .ConfigureAwait(false);
                    resource = reply?.Resource;
                    if (resource == null) {
                        return;
                    }
                    ResponseUtils.CheckResponse(reply.StatusCode, HttpStatusCode.OK);
                }
                catch (DocumentClientException dce)
                    when (dce.StatusCode == HttpStatusCode.NotFound) {
                    return;
                }

                if (resource.GetPropertyValue<bool>("_isDeleted")) {
                    return;
                }
                resource.SetPropertyValue("ttl", 300);
                resource.SetPropertyValue("_isDeleted", true);

                if (requestOptions == null) {
                    requestOptions = new RequestOptions();
                }
                requestOptions.AccessCondition = new AccessCondition {
                    Condition = resource.ETag,
                    Type = AccessConditionType.IfMatch
                };
                var response = await Client.ReplaceDocumentAsync(resource.SelfLink, resource,
                    requestOptions).ConfigureAwait(false);
                ResponseUtils.CheckResponse(response.StatusCode, HttpStatusCode.OK,
                    HttpStatusCode.Created);
            }, ResponseUtils.ContinueWithPrecondition, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Force delete item without change feed notification
        /// </summary>
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task ForceDeleteAsync(string id, string partitionKey, CancellationToken ct) {
            var uri = UriFactory.CreateDocumentUri(DatabaseId, Collection.Id, id);
            await Retry.Do(ct, async () => {
                try {
                    var response = await Client.DeleteDocumentAsync(uri, CreateRequestOptions(partitionKey))
                        .ConfigureAwait(false);
                    ResponseUtils.CheckResponse(response.StatusCode,
                        HttpStatusCode.NotFound, HttpStatusCode.NoContent);
                }
                catch (DocumentClientException dce)
                    when (dce.StatusCode == HttpStatusCode.NotFound) {
                    // Not found - continue
                }
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Bulk add or delete
        /// </summary>
        /// <param name="changes"></param>
        /// <param name="partitionKey"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task RunBulkUpdateAsync(IDocumentFeed<JObject> changes,
            string partitionKey, CancellationToken ct) {
            var uri = UriFactory.CreateStoredProcedureUri(DatabaseId, Collection.Id,
                DocumentServer.BulkUpdateSprocName);
            var max = _maxArgs;
            while (changes.HasMore()) {
                var items = await changes.ReadAsync(ct)
                    .ConfigureAwait(false);
                await RunBulkUpdateAsync(items, max, uri, partitionKey, ct)
                    .ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Bulk add or delete
        /// </summary>
        /// <param name="items"></param>
        /// <param name="partitionKey"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task RunBulkUpdateAsync(IEnumerable<JObject> items,
            string partitionKey, CancellationToken ct) {
            var uri = UriFactory.CreateStoredProcedureUri(DatabaseId, Collection.Id,
                DocumentServer.BulkUpdateSprocName);
            return RunBulkUpdateAsync(items, _maxArgs, uri, partitionKey, ct);
        }

        /// <summary>
        /// Bulk add or delete
        /// </summary>
        /// <param name="items"></param>
        /// <param name="max"></param>
        /// <param name="uri"></param>
        /// <param name="partitionKey"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        private async Task RunBulkUpdateAsync(IEnumerable<JObject> items, int max, Uri uri,
            string partitionKey, CancellationToken ct) {
            do {
                await Retry.Do(ct, async () => {
                    var bulk = items.Take(max).ToArray();
                    Console.WriteLine($"Changing {bulk.Length} items...");
                    var scriptResult = await Client.ExecuteStoredProcedureAsync<int>(
                        uri, CreateRequestOptions(partitionKey), bulk).ConfigureAwait(false);
                    Console.WriteLine($"  {scriptResult.Response} items changed...");
                    items = items.Skip(scriptResult.Response);
                    if (scriptResult.Response > 100) {
                        max = (int)(scriptResult.Response * 1.05);
                    }
                }, ex => {
                    if (ex is DocumentClientException dce) {
                        if (dce.StatusCode == HttpStatusCode.RequestEntityTooLarge ||
                            dce.StatusCode == HttpStatusCode.RequestTimeout) {
                            max = (int)(max * 0.7);
                        }
                    }
                    return true;
                }, ResponseUtils.CustomRetry, 500).ConfigureAwait(false);
            }
            while (items.Any());
        }

        /// <summary>
        /// Bulk delete
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="partitionKey"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task RunBulkDeleteAsync(SqlQuerySpec query,
            string partitionKey, CancellationToken ct) {
            var uri = UriFactory.CreateStoredProcedureUri(DatabaseId, Collection.Id,
                DocumentServer.BulkDeleteSprocName);
            await Retry.Do(ct, async () => {
                while(true) {
                    dynamic scriptResult = await Client.ExecuteStoredProcedureAsync<dynamic>(
                        uri, CreateRequestOptions(partitionKey), query).ConfigureAwait(false);
                    Console.WriteLine($"  {scriptResult.deleted} items deleted");
                    if (!scriptResult.continuation) {
                        break;
                    }
                }
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }


        /// <summary>
        /// Create feed options
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        private static FeedOptions CreateFeedOptions(string partitionKey, int batchSize) {
            return new FeedOptions {
                MaxDegreeOfParallelism = 8,
                MaxItemCount = batchSize,
#if GRAPH_PARTITIONS
                PartitionKey = partitionKey != null ? new PartitionKey(partitionKey) : null,
                EnableCrossPartitionQuery = partitionKey == null
#else
                EnableCrossPartitionQuery = true
#endif
            };
        }

        /// <summary>
        /// Create request options
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        private RequestOptions CreateRequestOptions(string partitionKey) {
#if GRAPH_PARTITIONS
            if (partitionKey != null) {
                return new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
            }
#endif
            return null;
        }

        public void Dispose() { }

        protected const int _retries = 100;
        protected const int _maxArgs = 5000;
    }
}
