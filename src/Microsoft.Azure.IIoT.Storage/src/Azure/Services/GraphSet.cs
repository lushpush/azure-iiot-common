// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.Documents.Linq;
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlParameter = Microsoft.Azure.Documents.SqlParameter;
    using SqlQuerySpec = Microsoft.Azure.Documents.SqlQuerySpec;

    /// <summary>
    /// Graph class
    /// </summary>
    internal class GraphSet : GraphQuery, Storage.IDocumentCollection {

        /// <summary>
        /// The uri of the graph
        /// </summary>
        public Uri GraphUri {
            get;
        }

        /// <summary>
        /// Create graph
        /// </summary>
        /// <param name="graphUri"></param>
        /// <param name="documentSet"></param>
        /// <param name="ttl"></param>
        internal GraphSet(Uri graphUri, DocumentSet documentSet,
            int? ttl = null) {
            GraphUri = graphUri ??
                throw new ArgumentNullException(nameof(graphUri));
            _documentSet = documentSet;
            _ttl = ttl;
        }

        /// <summary>
        /// Get changes for this graph set
        /// </summary>
        /// <returns></returns>
        public override IDocumentFeed<Change<Statement>> GetChanges() =>
            new ChangeFeed<Statement>(_documentSet.Processor, o => o.ToStatement(), o =>
                (o.TryGetValue("_isEdge", out var tmp) &&
                 o.TryGetValue("graph", out var graph) &&
                 ((string)graph) == GraphUri.AbsoluteUri));

        /// <summary>
        /// Adds statement
        /// </summary>
        /// <param name="statement"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task AddStatementAsync(Statement statement,
            CancellationToken ct) {

            await _documentSet.UpdateAsync(statement.ToString().ToSha1Hash(), existing =>
                statement.ToJson(GraphUri, _ttl), GraphUri.AbsoluteUri, ct)
                    .ConfigureAwait(false);
            // TODO Only add/delete, never update
            await _documentSet.UpdateAsync(statement.Subject.Id, existing =>
                existing != null ? null :
                    statement.Subject.ToJson(_ttl),
                GraphUri.AbsoluteUri, ct).ConfigureAwait(false);
            await _documentSet.UpdateAsync(statement.Object.Id, existing =>
                existing != null ? null :
                    statement.Object.ToJson(_ttl),
                GraphUri.AbsoluteUri, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Remove statement
        /// </summary>
        /// <param name="statement"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task DeleteStatementAsync(Statement statement,
            CancellationToken ct) {
            var s = new Statement(statement, GraphUri);
            await _documentSet.DeleteAsync(s.ToString().ToSha1Hash(),
                GraphUri.AbsoluteUri, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Bulk changes starting with removal
        /// </summary>
        /// <param name="changes"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task BulkChangeAsync(IDocumentFeed<Change<Statement>> changes,
            CancellationToken ct) {
            if (changes != null) {
                var feed = new BulkUpdateFeed(changes, GraphUri, _ttl);
                await _documentSet.RunBulkUpdateAsync(
                    feed, GraphUri.AbsoluteUri, ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Drop all content in graph
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task DropAllAsync(CancellationToken ct) {

            // TODO: Change delete script to be specific about graph delete

            return _documentSet.RunBulkDeleteAsync(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble + "ARRAY_CONTAINS(s.graph, @graph)",
                Parameters = {
                    new SqlParameter { Name = "@graph", Value = GraphUri.AbsoluteUri }
                }
            }, GraphUri.AbsoluteUri, ct);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose() {
            // _documentSet.Dispose();
        }

        /// <summary>
        /// Create graph scoped sql query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        protected override IDocumentQuery<T> GetSqlQuery<T>(
            SqlQuerySpec query, int batchSize) {
            query.QueryText += " and ARRAY_CONTAINS(s.graph, @graph)";
            query.Parameters.Add(
                new SqlParameter { Name = "@graph", Value = GraphUri.AbsoluteUri });
            return _documentSet.CreateSqlQuery<T>(query, GraphUri.AbsoluteUri, batchSize);
        }

        /// <summary>
        /// Create graph scoped gremlin query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        protected override IDocumentQuery<T> GetGremlinQuery<T>(
            string query, int batchSize) {
            if (query.IndexOf("g.V().", StringComparison.Ordinal) < 0 &&
                query.IndexOf("g.E().", StringComparison.Ordinal) < 0) {
                throw new ArgumentException(nameof(query));
            }
            query = query.Insert(5, $".has('graph', '{GraphUri.AbsoluteUri}')");
            return _documentSet.CreateGremlinQuery<T>(query, GraphUri.AbsoluteUri, batchSize);
        }

        protected override int DefaultPageSize => int.MaxValue;

        private readonly DocumentSet _documentSet;
        private readonly int? _ttl;
    }
}
