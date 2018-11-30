// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Azure.Services {
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Collection abstraction
    /// </summary>
    internal class CosmosDbCollection : IDocumentCollection {

        /// <summary>
        /// Returns collection
        /// </summary>
        internal DocumentCollection Collection { get; private set; }

        /// <summary>
        /// Create collection
        /// </summary>
        /// <param name="db"></param>
        /// <param name="collection"></param>
        /// <param name="logger"></param>
        internal CosmosDbCollection(CosmosDbDatabase db, DocumentCollection collection,
            ILogger logger) {
            _logger = logger;
            _db = db;
            Collection = collection;
        }

        /// <inheritdoc/>
        public async Task<dynamic> GetAsync(string id, CancellationToken ct) {
            try {
                return await Retry.Do(_logger, ct, () => _db.Client.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id), null, ct),
                    ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, kMaxRetries);
            }
            catch (DocumentClientException e) {
                if (e.StatusCode == HttpStatusCode.NotFound) {
                    return null;
                }
                throw;
            }
        }

        /// <inheritdoc/>
        public IDocumentFeed QueryAsync(Expression<Func<dynamic, bool>> queryExpression) {
            var query = _db.Client.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                   new FeedOptions {
                       MaxDegreeOfParallelism = 8,
                       MaxItemCount = -1,
                       EnableCrossPartitionQuery = true
                   })
                .Where(queryExpression)
                .AsDocumentQuery();
            return new CosmosDbFeed(query, _logger);
        }

        /// <inheritdoc/>
        public async Task<dynamic> UpsertAsync(dynamic item, string eTag,
            CancellationToken ct) {
            var ac = string.IsNullOrEmpty(eTag) ? null : new RequestOptions {
                AccessCondition = new AccessCondition {
                    Condition = eTag,
                    Type = AccessConditionType.IfMatch
                }
            };
            return await Retry.Do(_logger, ct, () => _db.Client.UpsertDocumentAsync(
                UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                    item, ac, false, ct),
                ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, kMaxRetries);
        }

        /// <inheritdoc/>
        public async Task DeleteAsync(string id, string eTag, CancellationToken ct) {
            var ac = string.IsNullOrEmpty(eTag) ? null : new RequestOptions {
                AccessCondition = new AccessCondition {
                    Condition = eTag,
                    Type = AccessConditionType.IfMatch
                }
            };
            await Retry.Do(_logger, ct, () => _db.Client.DeleteDocumentAsync(
                UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id), ac, ct),
                ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, kMaxRetries);
        }

        /// <summary>
        /// Bulk add or delete
        /// </summary>
        /// <param name="changes"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task RunBulkUpdateAsync(IDocumentFeed changes, CancellationToken ct) {
            var uri = UriFactory.CreateStoredProcedureUri(_db.DatabaseId, Collection.Id,
                CosmosDbDatabase.kBulkUpdateSprocName);
            var max = kMaxArgs;
            while (changes.HasMore()) {
                var items = await changes.ReadAsync(ct);
                await RunBulkUpdateAsync(items, max, uri, ct);
            }
        }

        /// <summary>
        /// Bulk add or delete
        /// </summary>
        /// <param name="items"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task RunBulkUpdateAsync(IEnumerable<dynamic> items, CancellationToken ct) {
            var uri = UriFactory.CreateStoredProcedureUri(_db.DatabaseId, Collection.Id,
                CosmosDbDatabase.kBulkUpdateSprocName);
            return RunBulkUpdateAsync(items, kMaxArgs, uri,  ct);
        }

        /// <summary>
        /// Bulk delete
        /// </summary>
        /// <param name="query"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task RunBulkDeleteAsync(SqlQuerySpec query, CancellationToken ct) {
            var uri = UriFactory.CreateStoredProcedureUri(_db.DatabaseId, Collection.Id,
                CosmosDbDatabase.kBulkDeleteSprocName);
            await Retry.Do(_logger, ct, async () => {
                while (true) {
                    dynamic scriptResult = await _db.Client.ExecuteStoredProcedureAsync<dynamic>(
                        uri, null, query);
                    Console.WriteLine($"  {scriptResult.deleted} items deleted");
                    if (!scriptResult.continuation) {
                        break;
                    }
                }
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, kMaxRetries);
        }

        /// <summary>
        /// Bulk add or delete
        /// </summary>
        /// <param name="items"></param>
        /// <param name="max"></param>
        /// <param name="uri"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        private async Task RunBulkUpdateAsync(IEnumerable<dynamic> items, int max, Uri uri,
            CancellationToken ct) {
            do {
                await Retry.Do(_logger, ct, async () => {
                    var bulk = items.Take(max).ToArray();
                    Console.WriteLine($"Changing {bulk.Length} items...");
                    var scriptResult = await _db.Client.ExecuteStoredProcedureAsync<int>(
                        uri, null, bulk);
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
                }, ResponseUtils.CustomRetry, kMaxRetries);
            }
            while (items.Any());
        }

        /// <summary>
        /// Soft delete item
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task SoftDeleteAsync(string id, CancellationToken ct) {
            var uri = UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id);
            await Retry.Do(_logger, ct, async () => {
                Resource resource = null;
                try {
                    var reply = await _db.Client.ReadDocumentAsync(uri);
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
                var requestOptions = new RequestOptions {
                    AccessCondition = new AccessCondition {
                        Condition = resource.ETag,
                        Type = AccessConditionType.IfMatch
                    }
                };
                var response = await _db.Client.ReplaceDocumentAsync(resource.SelfLink,
                    resource, requestOptions);
                ResponseUtils.CheckResponse(response.StatusCode, HttpStatusCode.OK,
                    HttpStatusCode.Created);
            }, ResponseUtils.ContinueWithPrecondition, ResponseUtils.CustomRetry, kMaxRetries);
        }

        private readonly CosmosDbDatabase _db;
        private readonly ILogger _logger;
        private const int kMaxRetries = 100;
        private const int kMaxArgs = 5000;
    }
}
