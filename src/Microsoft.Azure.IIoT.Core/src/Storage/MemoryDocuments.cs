// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.Utils;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// In memory database service (for testing)
    /// </summary>
    public class MemoryDocuments : IDocumentServer {

        /// <summary>
        /// Create service
        /// </summary>
        /// <param name="logger"></param>
        public MemoryDocuments(ILogger logger) {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public Task<IDocumentDatabase> OpenAsync(string id) {
            return Task.FromResult<IDocumentDatabase>(_databases.GetOrAdd(id,
                k => new MemoryDatabase(_logger)));
        }

        /// <summary>
        /// In memory database
        /// </summary>
        private class MemoryDatabase : IDocumentDatabase {

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
            public Task<IDocumentCollection> OpenCollectionAsync(string id) {
                return Task.FromResult<IDocumentCollection>(_collections.GetOrAdd(id,
                    k => new MemoryCollection(_logger)));
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
            public Task<dynamic> AddAsync(dynamic newItem, CancellationToken ct) {
                var newDoc = new ItemWrapper(newItem);
                lock (_data) {
                    if (_data.TryGetValue(newDoc.Id, out var doc)) {
                        return Task.FromException<dynamic>(
                            new ConflictingResourceException(newDoc.Id));
                    }
                    _data.Add(newDoc.Id, newDoc);
                    _logger.Info($"Added \n{newDoc}");
                    return Task.FromResult<dynamic>(newItem);
                }
            }

            /// <inheritdoc/>
            public Task DeleteAsync(dynamic itemOrId, CancellationToken ct,
                string etag) {
                var newDoc = new ItemWrapper(itemOrId);
                lock (_data) {
                    if (!_data.TryGetValue(newDoc.Id, out var doc)) {
                        return Task.FromException<dynamic>(
                            new ResourceNotFoundException(newDoc.Id));
                    }
                    if (string.IsNullOrEmpty(etag)) {
                        etag = newDoc.Etag;
                    }
                    if (!string.IsNullOrEmpty(etag) && doc.Etag != etag) {
                        return Task.FromException<dynamic>(
                            new ResourceOutOfDateException(etag));
                    }
                    _data.Remove(newDoc.Id);
                    _logger.Info($"Removed \n{newDoc}");
                    return Task.CompletedTask;
                }
            }

            /// <inheritdoc/>
            public Task<dynamic> GetAsync(string id, CancellationToken ct) {
                lock (_data) {
                    if (!_data.TryGetValue(id, out var item)) {
                        return null;
                    }
                    return Task.FromResult<dynamic>(item.Value);
                }
            }

            /// <inheritdoc/>
            public IDocumentFeed Query<T>(Func<IQueryable<T>, IQueryable<dynamic>> query,
                int? pageSize) {
                var results = query(_data.Values
                    .Select(d => d.Value.ToObject<T>())
                    .AsQueryable())
                    .AsEnumerable();
                var feed = (pageSize == null) ?
                    results.YieldReturn() : results.Batch(pageSize.Value);
                return new MemoryFeed(new Queue<IEnumerable<dynamic>>(feed));
            }

            /// <inheritdoc/>
            public Task<dynamic> ReplaceAsync(dynamic itemOrId, dynamic newItem,
                CancellationToken ct, string etag) {
                var newDoc = new ItemWrapper(newItem);
                lock (_data) {
                    if (_data.TryGetValue(newDoc.Id, out var doc)) {
                        if (string.IsNullOrEmpty(etag)) {
                            etag = newDoc.Etag;
                        }
                        if (!string.IsNullOrEmpty(etag) && doc.Etag != etag) {
                            return Task.FromException<dynamic>(
                                new ResourceOutOfDateException(etag));
                        }
                        _data.Remove(newDoc.Id);
                    }
                    else {
                        return Task.FromException<dynamic>(
                            new ResourceNotFoundException(newDoc.Id));
                    }
                    _data.Add(newDoc.Id, newDoc);
                    _logger.Info($"Replaced \n{newDoc}");
                    return Task.FromResult<dynamic>(newItem);
                }
            }

            /// <inheritdoc/>
            public Task<dynamic> UpsertAsync(dynamic newItem, CancellationToken ct,
                string etag) {
                var newDoc = new ItemWrapper(newItem);
                lock (_data) {
                    if (_data.TryGetValue(newDoc.Id, out var doc)) {
                        if (string.IsNullOrEmpty(etag)) {
                            etag = newDoc.Etag;
                        }
                        if (!string.IsNullOrEmpty(etag) && doc.Etag != etag) {
                            return Task.FromException<dynamic>(
                                new ResourceOutOfDateException(etag));
                        }
                        _data.Remove(newDoc.Id);
                    }
                    _logger.Info($"Upserted \n{newDoc}");
                    _data.Add(newDoc.Id, newDoc);
                    return Task.FromResult<dynamic>(newItem);
                }
            }

            /// <summary>
            /// Wraps a document value
            /// </summary>
            private struct ItemWrapper {

                /// <summary>
                /// Create memory document
                /// </summary>
                /// <param name="value"></param>
                public ItemWrapper(dynamic value) {
                    Value = value;
                }

                /// <summary>
                /// Value
                /// </summary>
                public JToken Value { get; }

                /// <summary>
                /// Etag
                /// </summary>
                public string Etag {
                    get {
                        if (Value is JObject o) {
                            if (!o.TryGetValue("__etag", out var etag)) {
                                etag = Guid.NewGuid().ToString();
                                o.Add("__etag", etag);
                            }
                            return (string)etag;
                        }
                        return null;
                    }
                }

                /// <summary>
                /// Identifier of the document
                /// </summary>
                public string Id {
                    get {
                        if (Value is JToken s) {
                            return s.Type == JTokenType.String ?
                                (string)s : null;
                        }
                        if (Value is JObject o) {
                            if (!o.TryGetValue("Id", out var id)) {
                                if (!o.TryGetValue("__id", out id)) {
                                    id = Guid.NewGuid().ToString();
                                    o.Add("__id", id);
                                }
                            }
                            return (string)id;
                        }
                        return null;
                    }
                }

                /// <inheritdoc/>
                public override bool Equals(object obj) {
                    if (obj is ItemWrapper wrapper) {
                        return JToken.DeepEquals(Value, wrapper.Value);
                    }
                    return false;
                }

                /// <inheritdoc/>
                public override int GetHashCode() =>
                    EqualityComparer<JToken>.Default.GetHashCode(Value);

                /// <inheritdoc/>
                public override string ToString() =>
                    Value.ToString(Newtonsoft.Json.Formatting.Indented);

                /// <inheritdoc/>
                public static bool operator ==(ItemWrapper o1, ItemWrapper o2) =>
                    o1.Equals(o2);

                /// <inheritdoc/>
                public static bool operator !=(ItemWrapper o1, ItemWrapper o2) =>
                    !(o1 == o2);
            }

            /// <summary>
            /// Memory feed
            /// </summary>
            private class MemoryFeed : IDocumentFeed {

                /// <summary>
                /// Create feed
                /// </summary>
                /// <param name="items"></param>
                public MemoryFeed(Queue<IEnumerable<dynamic>> items) {
                    _items = items;
                }

                /// <inheritdoc/>
                public void Dispose() { }

                /// <inheritdoc/>
                public bool HasMore() => _items.Count != 0;

                /// <inheritdoc/>
                public Task<IEnumerable<dynamic>> ReadAsync(CancellationToken ct) =>
                    Task.FromResult(_items.Dequeue());

                private readonly Queue<IEnumerable<dynamic>> _items;
            }

            private readonly Dictionary<string, ItemWrapper> _data =
                new Dictionary<string, ItemWrapper>();
            private ILogger _logger;
        }

        private readonly ConcurrentDictionary<string, MemoryDatabase> _databases =
            new ConcurrentDictionary<string, MemoryDatabase>();
        private readonly ILogger _logger;
    }
}
