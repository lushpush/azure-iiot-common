// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Azure.Services {
    using Microsoft.Azure.IIoT.Http.Exceptions;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.Documents;
    using System;
    using System.Linq;
    using System.Net;

    /// <summary>
    /// Document collection wrapper
    /// </summary>
    public static class ResponseUtils {

        /// <summary>
        /// Check response status code
        /// </summary>
        /// <param name="status"></param>
        /// <param name="expected"></param>
        public static void CheckResponse(HttpStatusCode status, params HttpStatusCode[] expected) {
            if (!expected.Contains(status)) {
                throw new HttpResponseException(status, null);
            }
        }

        /// <summary>
        /// Helper to decide whether to retry or throw
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static bool ContinueWithPrecondition(Exception ex) =>
            ShouldContinue(ex, true);

        /// <summary>
        /// Helper to decide whether to retry or throw
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static bool ShouldContinue(Exception ex) =>
            ShouldContinue(ex, false);

        /// <summary>
        /// Helper to decide whether to retry or throw
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="withPrecondition"></param>
        /// <returns></returns>
        public static bool ShouldContinue(Exception ex, bool withPrecondition) {
            HttpStatusCode statusCode;
            if (ex is ITransientException) {
                return true;
            }
            if (ex is HttpResponseException re) {
                statusCode = re.StatusCode;
            }
            else if (ex is DocumentClientException dce && dce.StatusCode.HasValue) {
                statusCode = dce.StatusCode.Value;
            }
            else {
                return false;
            }
            if (statusCode == HttpStatusCode.OK ||
                statusCode == (HttpStatusCode)429) {
                return true;
            }
            if (withPrecondition &&
                statusCode == HttpStatusCode.PreconditionFailed) {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Custom retry policy
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static int CustomRetry(int count, Exception ex) {
            var retryAfter = count * 100;
            if (ex is DocumentClientException dce) {
                if (dce.RetryAfter.TotalMilliseconds > 0) {
                    retryAfter = (int)dce.RetryAfter.TotalMilliseconds;
                }
            }
            return retryAfter;
        }
    }
}
