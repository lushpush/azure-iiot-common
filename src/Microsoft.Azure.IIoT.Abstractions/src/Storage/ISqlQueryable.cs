// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using System.Collections.Generic;

    /// <summary>
    /// Provides sql query capability
    /// </summary>
    public interface ISqlQueryable {

        /// <summary>
        /// Query items
        /// </summary>
        /// <param name="queryString"></param>
        /// <param name="parameters"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        IDocumentFeed Query(string queryString,
            IDictionary<string, object> parameters = null,
            int? pageSize = null);
    }
}
