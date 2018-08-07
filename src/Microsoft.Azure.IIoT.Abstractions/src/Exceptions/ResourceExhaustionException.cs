// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Exceptions {
    using System;

    /// <summary>
    /// This exception is thrown a resource, such as database, queue, or
    /// CPU is exhausted.
    /// </summary>
    public class ResourceExhaustionException : Exception {

        /// <inheritdoc />
        public ResourceExhaustionException() {
        }

        /// <inheritdoc />
        public ResourceExhaustionException(string message) :
            base(message) {
        }

        /// <inheritdoc />
        public ResourceExhaustionException(string message, Exception innerException) :
            base(message, innerException) {
        }
    }
}