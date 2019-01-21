// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Hub.Clients {
    using Microsoft.Azure.IIoT.Hub;
    using Microsoft.Azure.IIoT.Hub.Models;
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.EventHubs;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Event hub namespace client
    /// </summary>
    public sealed class EventHubNamespaceClient : IEventHubNamespace {

        /// <summary>
        /// Create service client
        /// </summary>
        /// <param name="config"></param>
        /// <param name="logger"></param>
        public EventHubNamespaceClient(IEventHubConfig config, ILogger logger) {
            if (string.IsNullOrEmpty(config.EventHubConnString)) {
                throw new ArgumentException(nameof(config));
            }
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <inheritdoc/>
        public Task<IEventHubClient> OpenAsync(string name) {
            var cs = new EventHubsConnectionStringBuilder(_config.EventHubConnString) {
                EntityPath = name ?? _config.EventHubPath
            }.ToString();
            var client = EventHubClient.CreateFromConnectionString(cs);
            return Task.FromResult<IEventHubClient>(new EventHubClientWrapper(client));
        }

        /// <summary>
        /// Wraps an event hub sdk client
        /// </summary>
        private class EventHubClientWrapper : IEventHubClient {

            /// <summary>
            /// Create wrapper
            /// </summary>
            /// <param name="client"></param>
            public EventHubClientWrapper(EventHubClient client) {
                _client = client;
            }

            /// <inheritdoc/>
            public Task SendAsync(EventModel message) {
                var ev = new EventData(Encoding.UTF8.GetBytes(message.Payload.ToString()));
                if (message.Properties != null) {
                    foreach (var prop in message.Properties) {
                        ev.Properties.Add(prop.Key, prop.Value);
                    }
                }
                return _client.SendAsync(ev);
            }

            /// <inheritdoc/>
            public Task SendAsync(byte[] data, string contentType) =>
                _client.SendAsync(CreateEvent(data, contentType));

            /// <inheritdoc/>
            public Task SendAsync(IEnumerable<byte[]> batch, string contentType) =>
                _client.SendAsync(batch.Select(b => CreateEvent(b, contentType)));

            /// <summary>
            /// Helper to create event from buffer and content type
            /// </summary>
            /// <param name="data"></param>
            /// <param name="contentType"></param>
            /// <returns></returns>
            private static EventData CreateEvent(byte[] data, string contentType) {
                var ev = new EventData(data);
                ev.Properties.Add(EventProperties.kContentType, contentType);
                return ev;
            }

            private readonly EventHubClient _client;
        }

        private readonly object _logger;
        private readonly IEventHubConfig _config;
    }
}
