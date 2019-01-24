// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Default {
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Exceptions;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// In memory database service (for testing)
    /// </summary>
    public sealed class MemoryServer : IDatabaseServer {

        /// <summary>
        /// Create service
        /// </summary>
        /// <param name="logger"></param>
        public MemoryServer(ILogger logger) {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public Task<IDatabase> OpenAsync(string id) {
            return Task.FromResult<IDatabase>(_databases.GetOrAdd(id ?? "",
                k => new MemoryDatabase(_logger)));
        }

        /// <summary>
        /// In memory database
        /// </summary>
        private class MemoryDatabase : IDatabase {

            /// <summary>
            /// Create service
            /// </summary>
            /// <param name="logger"></param>
            public MemoryDatabase(ILogger logger) {
                _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            }

            /// <inheritdoc/>
            public Task DeleteCollectionAsync(string id) {
                _collections.TryRemove(id, out var tmp);
                return Task.CompletedTask;
            }

            /// <inheritdoc/>
            public Task<IEnumerable<string>> ListCollectionsAsync(CancellationToken ct) {
                return Task.FromResult<IEnumerable<string>>(_collections.Keys);
            }

            /// <inheritdoc/>
            public Task<IDocumentCollection> OpenDocumentCollectionAsync(string id, bool partitioned) {
                return Task.FromResult<IDocumentCollection>(_collections.GetOrAdd(id ?? "",
                    k => new MemoryCollection(_logger)));
            }

            /// <inheritdoc/>
            public Task<IGraph> OpenGraphCollectionAsync(string id, bool partitioned) {
                throw new NotSupportedException();
            }

            private readonly ConcurrentDictionary<string, MemoryCollection> _collections =
                new ConcurrentDictionary<string, MemoryCollection>();
            private readonly ILogger _logger;
        }

        /// <summary>
        /// In memory collection
        /// </summary>
        private class MemoryCollection : IDocumentCollection {

            /// <summary>
            /// Create service
            /// </summary>
            /// <param name="logger"></param>
            public MemoryCollection(ILogger logger) {
                _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            }

            /// <inheritdoc/>
            public Task<IDocument<T>> AddAsync<T>(T newItem,
                CancellationToken ct, string id, string partitionKey) {
                var newDoc = new Document<T>(id, newItem, partitionKey);
                lock (_data) {
                    if (_data.TryGetValue(newDoc.Id, out var existing)) {
                        return Task.FromException<IDocument<T>>(
                            new ConflictingResourceException(newDoc.Id));
                    }
                    AddDocument(newDoc);
                    return Task.FromResult<IDocument<T>>(newDoc);
                }
            }

            /// <inheritdoc/>
            public Task DeleteAsync<T>(IDocument<T> item, CancellationToken ct) {
                if (item == null) {
                    throw new ArgumentNullException(nameof(item));
                }
                return DeleteAsync(item.Id, ct, item.PartitionKey, item.Etag);
            }

            /// <inheritdoc/>
            public Task DeleteAsync(string id, CancellationToken ct,
                string partitionKey, string etag) {
                if (string.IsNullOrEmpty(id)) {
                    throw new ArgumentNullException(nameof(id));
                }
                lock (_data) {
                    if (!_data.TryGetValue(id, out var doc)) {
                        return Task.FromException(
                            new ResourceNotFoundException(id));
                    }
                    if (!string.IsNullOrEmpty(etag) && etag != doc.Etag) {
                        return Task.FromException<dynamic>(
                            new ResourceOutOfDateException(etag));
                    }
                    _data.Remove(id);
                    return Task.CompletedTask;
                }
            }

            /// <inheritdoc/>
            public Task<IDocument<T>> GetAsync<T>(string id, CancellationToken ct,
                string partitionKey) {
                if (string.IsNullOrEmpty(id)) {
                    throw new ArgumentNullException(nameof(id));
                }
                lock (_data) {
                    _data.TryGetValue(id, out var item);
                    return Task.FromResult(item as IDocument<T>);
                }
            }

            /// <inheritdoc/>
            public IResultFeed<R> Query<T, R>(Func<IQueryable<IDocument<T>>,
                IQueryable<R>> query, int? pageSize, string partitionKey) {
                var results = query(_data.Values
                    .OfType<IDocument<T>>()
                    .AsQueryable())
                    .AsEnumerable();
                var feed = (pageSize == null) ?
                    results.YieldReturn() : results.Batch(pageSize.Value);
                return new MemoryFeed<R>(new Queue<IEnumerable<R>>(feed));
            }

            /// <inheritdoc/>
            public Task<IDocument<T>> ReplaceAsync<T>(IDocument<T> existing, T value,
                CancellationToken ct) {
                if (existing == null) {
                    throw new ArgumentNullException(nameof(existing));
                }
                var newDoc = new Document<T>(existing.Id, value, existing.PartitionKey);
                lock (_data) {
                    if (_data.TryGetValue(newDoc.Id, out var doc)) {
                        if (!string.IsNullOrEmpty(existing.Etag) && doc.Etag != existing.Etag) {
                            return Task.FromException<IDocument<T>>(
                                new ResourceOutOfDateException(existing.Etag));
                        }
                        _data.Remove(newDoc.Id);
                    }
                    else {
                        return Task.FromException<IDocument<T>>(
                            new ResourceNotFoundException(newDoc.Id));
                    }
                    AddDocument(newDoc);
                    return Task.FromResult<IDocument<T>>(newDoc);
                }
            }

            /// <inheritdoc/>
            public Task<IDocument<T>> UpsertAsync<T>(T newItem, CancellationToken ct,
                string id, string partitionKey, string etag) {
                var newDoc = new Document<T>(id, newItem, partitionKey);
                lock (_data) {
                    if (_data.TryGetValue(newDoc.Id, out var doc)) {
                        if (!string.IsNullOrEmpty(etag) && doc.Etag != etag) {
                            return Task.FromException<IDocument<T>>(
                                new ResourceOutOfDateException(etag));
                        }
                        _data.Remove(newDoc.Id);
                    }

                    AddDocument(newDoc);
                    return Task.FromResult<IDocument<T>>(newDoc);
                }
            }

            /// <summary>
            /// Checks size of newly added document
            /// </summary>
            /// <typeparam name="T"></typeparam>
            /// <param name="newDoc"></param>
            private void AddDocument<T>(Document<T> newDoc) {
                const int kMaxDocSize = 2 * 1024 * 1024;  // 2 meg like in cosmos
                var size = newDoc.Size;
                if (newDoc.Size > kMaxDocSize) {
                    throw new ResourceTooLargeException(newDoc.ToString(), size, kMaxDocSize);
                }
                _data.Add(newDoc.Id, newDoc);
#if LOG_VERBOSE
                _logger.Info($"{newDoc}");
#endif
            }

            /// <inheritdoc/>
            public ISqlQueryClient OpenSqlQueryClient() {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Wraps a document value
            /// </summary>
            private abstract class MemoryDocument {

                /// <summary>
                /// Returns the size of the document
                /// </summary>
                public int Size => System.Text.Encoding.UTF8.GetByteCount(
                    _value.ToString(Newtonsoft.Json.Formatting.None));

                /// <summary>
                /// Create memory document
                /// </summary>
                /// <param name="id"></param>
                /// <param name="value"></param>
                /// <param name="partitionKey"></param>
                protected MemoryDocument(JObject value, string id, string partitionKey) {
                    _value = value;
                    Id = id ?? _value.GetValueOrDefault("id", Guid.NewGuid().ToString());
                    PartitionKey = partitionKey ?? _value.GetValueOrDefault("__pk",
                        Guid.NewGuid().ToString());
                }

                /// <inheritdoc/>
                public string Id { get; }

                /// <inheritdoc/>
                public string PartitionKey { get; }

                /// <inheritdoc/>
                public string Etag { get; } = Guid.NewGuid().ToString();

                /// <inheritdoc/>
                public override bool Equals(object obj) {
                    if (obj is MemoryDocument wrapper) {
                        return JToken.DeepEquals(_value, wrapper._value);
                    }
                    return false;
                }

                /// <inheritdoc/>
                public override int GetHashCode() =>
                    EqualityComparer<JObject>.Default.GetHashCode(_value);

                /// <inheritdoc/>
                public override string ToString() =>
                    _value.ToString(Newtonsoft.Json.Formatting.Indented);

                /// <inheritdoc/>
                public static bool operator ==(MemoryDocument o1, MemoryDocument o2) =>
                    o1.Equals(o2);

                /// <inheritdoc/>
                public static bool operator !=(MemoryDocument o1, MemoryDocument o2) =>
                    !(o1 == o2);

                private readonly JObject _value;
            }


            /// <summary>
            /// Wraps a document value
            /// </summary>
            private class Document<T> : MemoryDocument, IDocument<T> {

                /// <summary>
                /// Create memory document
                /// </summary>
                /// <param name="id"></param>
                /// <param name="value"></param>
                /// <param name="partitionKey"></param>
                public Document(string id, T value, string partitionKey) :
                    base (JObject.FromObject(value), id, partitionKey) {
                    Value = value;
                }

                /// <inheritdoc/>
                public T Value { get; }
            }

            /// <summary>
            /// Memory feed
            /// </summary>
            private class MemoryFeed<T> : IResultFeed<T> {

                /// <summary>
                /// Create feed
                /// </summary>
                /// <param name="items"></param>
                public MemoryFeed(Queue<IEnumerable<T>> items) {
                    _items = items;
                }

                /// <inheritdoc/>
                public void Dispose() { }

                /// <inheritdoc/>
                public bool HasMore() => _items.Count != 0;

                /// <inheritdoc/>
                public Task<IEnumerable<T>> ReadAsync(CancellationToken ct) =>
                    Task.FromResult(HasMore() ? _items.Dequeue() : null);

                private readonly Queue<IEnumerable<T>> _items;
            }

            private readonly Dictionary<string, MemoryDocument> _data =
                new Dictionary<string, MemoryDocument>();
            private readonly ILogger _logger;
        }

        private readonly ConcurrentDictionary<string, MemoryDatabase> _databases =
            new ConcurrentDictionary<string, MemoryDatabase>();
        private readonly ILogger _logger;
    }
}
