// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Exceptions {
    using System;

    /// <inheritdoc />
    public class QueryException : StorageException {
        /// <inheritdoc />
        public QueryException(string message) :
            base(message) {
        }

        /// <inheritdoc />
        public QueryException(string message, Exception innerException) :
            base(message, innerException) {
        }
    }
}
