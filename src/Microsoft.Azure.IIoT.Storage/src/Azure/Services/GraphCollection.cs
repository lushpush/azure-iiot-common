// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.Documents.Linq;
    using SqlParameter = Azure.Documents.SqlParameter;
    using SqlQuerySpec = Azure.Documents.SqlQuerySpec;

    /// <summary>
    /// Wraps a document client
    /// </summary>
    internal class GraphCollection : GraphQuery, Storage.IDocumentCollection {

        /// <summary>
        /// Creates graph collection
        /// </summary>
        /// <param name="documentSet"></param>
        internal GraphCollection(DocumentSet documentSet) {
            _documentSet = documentSet;
        }

        /// <summary>
        /// Get changes
        /// </summary>
        /// <returns></returns>
        public override IDocumentFeed<Change<Statement>> GetChanges() =>
            new ChangeFeed<Statement>(_documentSet.Processor, o => o.ToStatement(), 
                o => o.TryGetValue("_isEdge", out var tmp));

        /// <summary>
        /// Opens a graph
        /// </summary>
        /// <param name="graphUri"></param>
        /// <returns></returns>
        public async Task<Storage.IDocumentCollection> OpenGraphAsync(Uri graphUri) {
            var id = graphUri.AbsoluteUri.ToSha1Hash();
            // TODO: Add id to graph table
            await Task.Delay(1).ConfigureAwait(false);
            return new GraphSet(graphUri, _documentSet);
        }

        /// <summary>
        /// Create temp graph
        /// </summary>
        /// <param name="ttl"></param>
        /// <returns></returns>
        public async Task<Storage.IDocumentCollection> CreateTempGraph(TimeSpan ttl) {
            var id = Guid.NewGuid().ToString();
            // TODO: Add id to graph table
            await Task.Delay(1).ConfigureAwait(false);
            return new GraphSet(new Uri("urn:" + id), _documentSet, 
                (int)ttl.TotalSeconds);
        }

        /// <summary>
        /// Read from graph collection
        /// </summary>
        /// <param name="handler"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task ListGraphsAsync(Action<IEnumerable<Uri>> handler, 
            CancellationToken ct) {

            // TODO: Implement
            return Task.CompletedTask;
        }

        /// <summary>
        /// Delete a graph with uri
        /// </summary>
        /// <param name="graphUri"></param>
        /// <returns></returns>
        public Task DeleteGraphAsync(Uri graphUri) {
            return _documentSet.RunBulkDeleteAsync(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble + "ARRAY_CONTAINS(s.graph, @graph)",
                Parameters = {
                    new SqlParameter { Name = "@graph", Value = graphUri.AbsoluteUri }
                }
            }, graphUri.AbsoluteUri, CancellationToken.None);
        }

        protected override int DefaultPageSize => int.MaxValue;

        /// <summary>
        /// Create collection scoped sql query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        protected override IDocumentQuery<T> GetSqlQuery<T>(
            SqlQuerySpec query, int batchSize) =>
            _documentSet.CreateSqlQuery<T>(query, null, batchSize);

        /// <summary>
        /// Create collection scoped gremlin query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        protected override IDocumentQuery<T> GetGremlinQuery<T>(
            string query, int batchSize) =>
            _documentSet.CreateGremlinQuery<T>(query, null, batchSize);

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose() {
            // _documentSet.Dispose();
        }

        private readonly DocumentSet _documentSet;
    }
}
