// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.IIoT.Storage;
    using Microsoft.Azure.Documents.Linq;
    using Newtonsoft.Json.Linq;
    using SqlParameter = Azure.Documents.SqlParameter;
    using SqlQuerySpec = Azure.Documents.SqlQuerySpec;

    /// <summary>
    /// Graph query base class for store and graph queries
    /// </summary>
    public abstract class GraphQuery : Storage.IDocumentQuery, IGremlinQuery, ISqlQuery {

        protected abstract int DefaultPageSize { get; }

        /// <summary>
        /// Create sql queryable
        /// </summary>
        /// <param name="query"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        protected abstract IDocumentQuery<T> GetSqlQuery<T>(
            SqlQuerySpec query, int batchSize);

        /// <summary>
        /// Create gremlin queryable
        /// </summary>
        /// <param name="query"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        protected abstract IDocumentQuery<T> GetGremlinQuery<T>(
            string query, int batchSize);

        /// <summary>
        /// Returns statements
        /// </summary>
        /// <returns></returns>
        public virtual IDocumentFeed<Change<Statement>> GetStatements(
            ChangeEventType type) => new DocumentFeed<JObject, Change<Statement>>(
                GetSqlQuery<JObject>(new SqlQuerySpec {
                    QueryText = _sqlQueryPreamble + "s._isEdge = true"
                }, DefaultPageSize), 
                i => i.Select(j => new Change<Statement>(type, ((JObject)j).ToStatement())));

        /// <summary>
        /// Get changes
        /// </summary>
        /// <returns></returns>
        public abstract IDocumentFeed<Change<Statement>> GetChanges();

        /// <summary>
        /// Create gremlin query
        /// </summary>
        /// <param name="gremlin"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public IDocumentFeed<JToken> GetGremlinResults(string gremlin, 
            int batchSize) => new DocumentFeed<JToken, JToken>(
                GetGremlinQuery<JToken>(gremlin, batchSize), t => t.Cast<JToken>());

        /// <summary>
        /// Create sql query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public IDocumentFeed<JToken> GetSqlResults(SqlQuerySpec query, 
            int batchSize) => new DocumentFeed<JToken, JToken>(
                GetSqlQuery<JToken>(query, batchSize), t => t.Cast<JToken>());

        /// <summary>
        /// Finds all statements with subject
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithSubject(Resource subject, 
            CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble + "s._vertexLabel = @subject",
                Parameters = {
                    new SqlParameter { Name = "@subject", Value = subject.ToString() },
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// Finds all statements with predicate
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithPredicate(string predicate, 
            CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble + "s.label = @predicate",
                Parameters = {
                    new SqlParameter { Name = "@predicate", Value = predicate }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// Returns all that contain object
        /// </summary>
        /// <param name="object"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithObject(Resource @object, 
            CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble + "s._sinkLabel = @object",
                Parameters = {
                    new SqlParameter { Name = "@object", Value = @object.ToString() }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// All with subject and predicate
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="predicate"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithSubjectPredicate(Resource subject,
            string predicate, CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble +
                    "s.label = @predicate and s._vertexLabel = @subject",
                Parameters = {
                    new SqlParameter { Name = "@subject", Value = subject.ToString() },
                    new SqlParameter { Name = "@predicate", Value = predicate }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// With subject and object
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="object"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithSubjectObject(Resource subject, 
            Resource @object, CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble +
                    "s._sinkLabel = @object and s._vertexLabel = @subject",
                Parameters = {
                    new SqlParameter { Name = "@subject", Value = subject.ToString() },
                    new SqlParameter { Name = "@object", Value = @object.ToString() }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// With predicate and object
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="object"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithPredicateObject(string predicate, 
            Resource @object, CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble +
                    "s.label = @predicate and s._sinkLabel = @object",
                Parameters = {
                    new SqlParameter { Name = "@object", Value = @object.ToString() },
                    new SqlParameter { Name = "@predicate", Value = predicate }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// With subject or object
        /// </summary>
        /// <param name="subjectOrObject"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithSubjectOrObject(Resource subjectOrObject, 
            CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble +
                    "(s._sinkLabel = @resource or s._vertexLabel = @resource)",
                Parameters = {
                    new SqlParameter { Name = "@resource", Value = subjectOrObject.ToString() }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// With subject or object
        /// </summary>
        /// <param name="subjectOrObject"></param>
        /// <param name="predicate"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithSubjectOrObject(Resource subjectOrObject,
            string predicate, CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble +
                    "s.label = @predicate and (s._sinkLabel = @resource or s._vertexLabel = @resource)",
                Parameters = {
                    new SqlParameter { Name = "@resource", Value = subjectOrObject.ToString() },
                    new SqlParameter { Name = "@predicate", Value = predicate }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// Gets statements containing the provided resource as subject
        /// or object and given predicate
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="predicate"></param>
        /// <param name="object"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> WithSubjectPredicateObject(Resource subject,
            string predicate, Resource @object, CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = _sqlQueryPreamble +
                    "s._sinkLabel = @object and s.label = @predicate and s._vertexLabel = @subject",
                Parameters = {
                    new SqlParameter { Name = "@subject", Value = subject.ToString() },
                    new SqlParameter { Name = "@predicate", Value = predicate },
                    new SqlParameter { Name = "@object", Value = @object.ToString() }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// Gets statements containing the provided resource as subject
        /// or object and given predicate
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<IEnumerable<Statement>> TopXStatements(int count,
            CancellationToken ct) =>
            GetSqlQuery<JObject>(new SqlQuerySpec {
                QueryText = "select TOP @count * from s " +
                    "where NOT IS_DEFINED(s._isDeleted) and s._isEdge = true",
                Parameters = {
                    new SqlParameter { Name = "@count", Value = count }
                }
            }, DefaultPageSize).AllAsync(ct);

        /// <summary>
        /// Return speciylization
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T GetSpecializedQuery<T>() where T : class => this as T;

        protected const string _sqlQueryPreamble = 
            "select * from s where NOT IS_DEFINED(s._isDeleted) and ";
    }
}
