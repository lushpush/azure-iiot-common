// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Azure.Services {
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

    /// <summary>
    /// Collection abstraction
    /// </summary>
    internal class CosmosDbCollection : IDocumentCollection, ISqlQueryable {

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
        public IDocumentFeed Query<T>(Func<IQueryable<T>, IQueryable<dynamic>> query,
            int? pageSize) {
            if (query == null) {
                throw new ArgumentNullException(nameof(query));
            }
            var result = query(_db.Client.CreateDocumentQuery<T>(
                UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                   new FeedOptions {
                       MaxDegreeOfParallelism = 8,
                       MaxItemCount = pageSize ?? - 1,
                       EnableCrossPartitionQuery = true
                   }));
            return new CosmosDbFeed(result.AsDocumentQuery(), _logger);
        }

        /// <inheritdoc/>
        public IDocumentFeed Query(string queryString, IDictionary<string, object> parameters,
            int? pageSize) {
            if (string.IsNullOrEmpty(queryString)) {
                throw new ArgumentNullException(nameof(queryString));
            }
            var query = _db.Client.CreateDocumentQuery(
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
                    EnableCrossPartitionQuery = true
                });
            return new CosmosDbFeed(query.AsDocumentQuery(), _logger);
        }

        /// <inheritdoc/>
        public async Task<dynamic> GetAsync(string id, CancellationToken ct) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            try {
                return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                    try {
                        return await _db.Client.ReadDocumentAsync(
                            UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id),
                            null, ct);
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
        public async Task<dynamic> UpsertAsync(dynamic newItem, CancellationToken ct,
            string eTag) {
            if (newItem == null) {
                throw new ArgumentNullException(nameof(newItem));
            }
            if (string.IsNullOrEmpty(eTag)) {
                eTag = GetEtagFromItem(newItem);
            }
            var ac = string.IsNullOrEmpty(eTag) ? null : new RequestOptions {
                AccessCondition = new AccessCondition {
                    Condition = eTag,
                    Type = AccessConditionType.IfMatch
                }
            };
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return await _db.Client.UpsertDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                        newItem, ac, false, ct);
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public async Task<dynamic> ReplaceAsync(dynamic itemOrId,
            dynamic newItem, CancellationToken ct, string eTag) {
            if (newItem == null) {
                throw new ArgumentNullException(nameof(newItem));
            }
            var id = GetIdFromItem(itemOrId);
            if (id == null) {
                throw new ArgumentException("Item is missing id");
            }
            if (string.IsNullOrEmpty(eTag)) {
                eTag = GetEtagFromItem(itemOrId);
            }
            var ac = string.IsNullOrEmpty(eTag) ? null : new RequestOptions {
                AccessCondition = new AccessCondition {
                    Condition = eTag,
                    Type = AccessConditionType.IfMatch
                }
            };
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return await _db.Client.ReplaceDocumentAsync(
                        UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id),
                        newItem, ac, ct);
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public async Task<dynamic> AddAsync(dynamic newItem, CancellationToken ct) {
            if (newItem == null) {
                throw new ArgumentNullException(nameof(newItem));
            }
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return await _db.Client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(_db.DatabaseId, Collection.Id),
                        newItem, null, false, ct);
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public async Task DeleteAsync(dynamic item, CancellationToken ct, string eTag) {
            var id = GetIdFromItem(item);
            if (id == null) {
                throw new ArgumentException("Item is missing id");
            }
            if (string.IsNullOrEmpty(eTag)) {
                eTag = GetEtagFromItem(item);
            }
            var ac = string.IsNullOrEmpty(eTag) ? null : new RequestOptions {
                AccessCondition = new AccessCondition {
                    Condition = eTag,
                    Type = AccessConditionType.IfMatch
                }
            };
            await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    await _db.Client.DeleteDocumentAsync(
                        UriFactory.CreateDocumentUri(_db.DatabaseId, Collection.Id, id),
                        ac, ct);
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
        /// Return etag from item
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        private static string GetEtagFromItem(dynamic item) =>
            Try.Op(() => (string)item.__etag);

        /// <summary>
        /// Return id from item
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        private static string GetIdFromItem(dynamic item) {
            var id = Try.Op(() => (string)item.Id);
            if (string.IsNullOrEmpty(id)) {
                id = Try.Op(() => (string)item.__id);
                if (string.IsNullOrEmpty(id)) {
                    id = Try.Op(() => (string)item);
                }
            }
            return id;
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

        private const int kMaxArgs = 5000;

        /// <summary>
        /// Bulk delete
        /// </summary>
        /// <param name="query"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task RunBulkDeleteAsync(SqlQuerySpec query, CancellationToken ct) {
            var uri = UriFactory.CreateStoredProcedureUri(_db.DatabaseId, Collection.Id,
                CosmosDbDatabase.kBulkDeleteSprocName);
            await Retry.WithoutDelay(_logger, ct, async () => {
                while (true) {
                    try {
                        dynamic scriptResult = await _db.Client.ExecuteStoredProcedureAsync<dynamic>(
                            uri, null, query);
                        Console.WriteLine($"  {scriptResult.deleted} items deleted");
                        if (!scriptResult.continuation) {
                            break;
                        }
                    }
                    catch (Exception ex) {
                        FilterException(ex);
                    }
                }
            });
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
                await Retry.WithoutDelay(_logger, ct, async () => {
                    try {
                        var bulk = items.Take(max).ToArray();
                        Console.WriteLine($"Changing {bulk.Length} items...");
                        var scriptResult = await _db.Client.ExecuteStoredProcedureAsync<int>(
                            uri, null, bulk);
                        Console.WriteLine($"  {scriptResult.Response} items changed...");
                        items = items.Skip(scriptResult.Response);
                        if (scriptResult.Response > 100) {
                            max = (int)(scriptResult.Response * 1.05);
                        }
                    }
                    catch (DocumentClientException dce) {
                        if (dce.StatusCode == HttpStatusCode.RequestEntityTooLarge ||
                            dce.StatusCode == HttpStatusCode.RequestTimeout) {
                            max = (int)(max * 0.7);
                        }
                        else {
                            FilterException(dce);
                        }
                    }
                    catch (Exception ex) {
                        FilterException(ex);
                    }
                });
            }
            while (items.Any());
        }

        private readonly CosmosDbDatabase _db;
        private readonly ILogger _logger;
    }
}
