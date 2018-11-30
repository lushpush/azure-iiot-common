// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {
    using System.Threading.Tasks;

    /// <summary>
    /// Document database service
    /// </summary>
    public interface IDocumentServer {

        /// <summary>
        /// Opens the database
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Task<IDocumentDatabase> OpenAsync(string id = null);
    }
}
