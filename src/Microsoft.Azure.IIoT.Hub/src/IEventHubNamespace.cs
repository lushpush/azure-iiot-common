// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Hub {
    using System.Threading.Tasks;

    /// <summary>
    /// Event hub namespace
    /// </summary>
    public interface IEventHubNamespace {

        /// <summary>
        /// Create client to event hub path in namespace.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        Task<IEventHubClient> OpenAsync(string path);
    }
}
