// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.IIoT.Utils;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    public static class DocumentQueryEx {

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <param name="query"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public static Task BatchProcessAsync<T>(this IQueryable<T> query,
            Action<IEnumerable<T>> handler, CancellationToken ct) =>
            BatchProcessAsync(query.AsDocumentQuery(), handler, ct);

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public static async Task BatchProcessAsync<T>(this IDocumentQuery<T> docQuery,
            Action<IEnumerable<T>> handler, CancellationToken ct) {
            await Retry.Do(ct, async () => {
                do {
                    handler(await docQuery.ExecuteNextAsync<T>(ct)
                        .ConfigureAwait(false));
                }
                while (docQuery.HasMoreResults);
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <returns></returns>
        public static async Task<IEnumerable<T>> AllAsync<T>(
            this IDocumentQuery<T> docQuery, CancellationToken ct) {
            var result = new List<T>();
            await docQuery.BatchProcessAsync(e => result.AddRange(e), ct)
                .ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <param name="query"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public static Task BatchProcessAsync<T>(this IQueryable<T> query,
            Func<IEnumerable<T>, Task> handler, CancellationToken ct) =>
            BatchProcessAsync(query.AsDocumentQuery(), handler, ct);

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public static async Task BatchProcessAsync<T>(this IDocumentQuery<T> docQuery,
            Func<IEnumerable<T>, Task> handler, CancellationToken ct) {
            await Retry.Do(ct, async () => {
                do {
                    await handler(await docQuery.ExecuteNextAsync<T>(ct)
                        .ConfigureAwait(false)).ConfigureAwait(false);
                }
                while (docQuery.HasMoreResults);
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <param name="query"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public static Task BatchProcessStatementsAsync(this IQueryable<JObject> query,
            Action<IEnumerable<Statement>> handler, CancellationToken ct) =>
            BatchProcessStatementsAsync(query.AsDocumentQuery(), handler, ct);

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <param name="docQuery"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        public static async Task BatchProcessStatementsAsync(this IDocumentQuery<JObject> docQuery,
            Action<IEnumerable<Statement>> handler, CancellationToken ct) {
            await Retry.Do(ct, async () => {
                do {
                    var results = await docQuery.ExecuteNextAsync<JObject>(ct)
                        .ConfigureAwait(false);
                    handler(results.Select(j => j.ToStatement()));
                }
                while (docQuery.HasMoreResults);
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Query and handle document batches
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <returns></returns>
        public static async Task<IEnumerable<Statement>> AllAsync(
            this IDocumentQuery<JObject> docQuery, CancellationToken ct) {
            var result = new List<Statement>();
            await docQuery.BatchProcessStatementsAsync(e => result.AddRange(e), ct)
                .ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Query one document and return it or default value of T
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Task<T> SingleOrDefaultAsync<T>(
            this IQueryable<T> query, CancellationToken ct) =>
            SingleOrDefaultAsync(query.AsDocumentQuery(), ct);

        /// <summary>
        /// Query one document and return it or default value of T
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <returns></returns>
        public static async Task<T> SingleOrDefaultAsync<T>(
            this IDocumentQuery<T> docQuery, CancellationToken ct) {
            return await Retry.Do(ct, async () => {
                while (true) {
                    var results = await docQuery.ExecuteNextAsync<T>(ct)
                        .ConfigureAwait(false);
                    if (!docQuery.HasMoreResults || results.Any()) {
                        return results.SingleOrDefault();
                    }
                }
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Query one document
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Task<T> SingleAsync<T>(
            this IQueryable<T> query, CancellationToken ct) =>
            SingleAsync(query.AsDocumentQuery(), () => new InvalidOperationException(), ct);

        /// <summary>
        /// Query one document
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <returns></returns>
        public static Task<T> SingleAsync<T>(
            this IDocumentQuery<T> docQuery, CancellationToken ct) =>
            SingleAsync(docQuery, () => new InvalidOperationException(), ct);


        /// <summary>
        /// Query one document and throw custom exception if not exactly one.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Task<T> SingleAsync<T>(
            this IQueryable<T> query, Func<Exception> thrower, CancellationToken ct) =>
            SingleAsync(query.AsDocumentQuery(), thrower, ct);

        /// <summary>
        /// Query one document and throw custom exception if not exactly one.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <returns></returns>
        public static async Task<T> SingleAsync<T>(
            this IDocumentQuery<T> docQuery, Func<Exception> thrower, CancellationToken ct) {
            return await Retry.Do(ct, async () => {
                while (true) {
                    var results = await docQuery.ExecuteNextAsync<T>(ct)
                        .ConfigureAwait(false);
                    if (!docQuery.HasMoreResults || results.Any()) {
                        if (results.Count() != 1) {
                            throw thrower();
                        }
                        return results.First();
                    }
                }
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Return whether there are any results
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Task<bool> AnyAsync<T>(this IQueryable<T> query,
            CancellationToken ct) => AnyAsync(query.AsDocumentQuery(), ct);

        /// <summary>
        /// Return whether there are any results
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docQuery"></param>
        /// <returns></returns>
        public static async Task<bool> AnyAsync<T>(this IDocumentQuery<T> docQuery,
            CancellationToken ct) {
            return await Retry.Do(ct, async () => {
                while(true) {
                    var results = await docQuery.ExecuteNextAsync<T>(ct)
                        .ConfigureAwait(false);
                    if (results.Any()) {
                        return true;
                    }
                    if (!docQuery.HasMoreResults) {
                        return false;
                    }
                }
            }, ResponseUtils.ShouldContinue, ResponseUtils.CustomRetry, int.MaxValue)
                .ConfigureAwait(false);
        }
    }
}
