// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Hub {
    using Microsoft.Azure.IIoT.Hub.Models;
    using System.Threading.Tasks;

    /// <summary>
    /// Event Hub Client
    /// </summary>
    public interface IEventHubClient : IEventClient {

        /// <summary>
        /// Send the provided message to event hub
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        Task SendAsync(EventModel message);
    }
}
