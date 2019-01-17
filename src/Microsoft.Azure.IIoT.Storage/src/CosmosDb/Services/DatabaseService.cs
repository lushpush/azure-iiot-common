// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.IIoT.Storage.CosmosDb;
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides document db and graph functionality for storage interfaces.
    /// </summary>
    public sealed class DatabaseService : IDatabaseServer {

        /// <summary>
        /// Creates server
        /// </summary>
        /// <param name="config"></param>
        /// <param name="logger"></param>
        public DatabaseService(ICosmosDbConfig config, ILogger logger) {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            if (string.IsNullOrEmpty(_config?.DbConnectionString)) {
                throw new ArgumentNullException(nameof(_config.DbConnectionString));
            }
        }

        /// <inheritdoc/>
        public async Task<IDatabase> OpenAsync(string databaseId) {
            if (string.IsNullOrEmpty(databaseId)) {
                databaseId = "default";
            }
            var cs = ConnectionString.Parse(_config.DbConnectionString);
            var client = new DocumentClient(new Uri(cs.Endpoint),
                cs.SharedAccessKey);
            await client.CreateDatabaseIfNotExistsAsync(new Database {
                Id = databaseId
            });
            return new DatabaseStore(client, databaseId, _logger);
        }

        private readonly ICosmosDbConfig _config;
        private readonly ILogger _logger;
    }
}
