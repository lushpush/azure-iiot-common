// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Hub {
    using Microsoft.Azure.IIoT.Hub.Models;
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Diagnostics.Models;
    using System.Threading.Tasks;
    using System;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Log entry writer based on event hub namespace
    /// </summary>
    public sealed class EventHubAuditLog : IAuditLog {

        /// <summary>
        /// Create event hub based audit log
        /// </summary>
        /// <param name="ns"></param>
        public EventHubAuditLog(IEventHubNamespace ns) {
            _ns = ns ?? throw new ArgumentNullException(nameof(ns));
        }

        /// <inheritdoc/>
        public async Task<IAuditLogWriter> OpenAsync(string log) {
            var client = await _ns.OpenAsync(log);
            return new EventHubWriter(client);
        }

        /// <summary>
        /// Client wrapper
        /// </summary>
        private class EventHubWriter : IAuditLogWriter {

            /// <summary>
            /// Create writer
            /// </summary>
            /// <param name="eventHub"></param>
            public EventHubWriter(IEventHubClient eventHub) {
                _eventHub = eventHub;
            }

            /// <inheritdoc/>
            public Task WriteAsync(AuditLogEntryModel entry) =>
                _eventHub.SendAsync(new EventModel {
                    Payload = JToken.FromObject(entry)
                });

            private readonly IEventHubClient _eventHub;
        }

        private readonly IEventHubNamespace _ns;
    }
}
