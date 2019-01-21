// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Hub.Processor.Runtime {
    using Microsoft.Azure.IIoT.Hub.Processor;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Extensions.Configuration;
    using System;

    /// <summary>
    /// Event processor configuration - wraps a configuration root
    /// </summary>
    public class EventProcessorConfig : ConfigBase, IEventProcessorConfig {

        /// <summary>
        /// Event processor configuration
        /// </summary>
        private const string kReceiveBatchSizeKey = "ReceiveBatchSize";
        private const string kReceiveTimeoutKey = "ReceiveTimeout";
        private const string kNamespaceKey = "StorageNamespace";
        private const string kBlobStorageConnStringKey = "BlobStorageConnectionString";
        private const string kLeaseContainerNameKey = "LeaseContainerName";

        /// <summary> Checkpoint storage </summary>
        public string BlobStorageConnString {
            get {
                var account = GetStringOrDefault("PCS_ASA_DATA_AZUREBLOB_ACCOUNT",
                    GetStringOrDefault("PCS_IOTHUBREACT_AZUREBLOB_ACCOUNT", null));
                var key = GetStringOrDefault("PCS_ASA_DATA_AZUREBLOB_KEY",
                    GetStringOrDefault("PCS_IOTHUBREACT_AZUREBLOB_KEY", null));
                var suffix = GetStringOrDefault("PCS_ASA_DATA_AZUREBLOB_ENDPOINT_SUFFIX",
                    GetStringOrDefault("PCS_IOTHUBREACT_AZUREBLOB_ENDPOINT_SUFFIX", "core.windows.net"));
                if (string.IsNullOrEmpty(account) || string.IsNullOrEmpty(key)) {
                    var cs = GetStringOrDefault(kBlobStorageConnStringKey, GetStringOrDefault(
                        _serviceId + "_STORE_CS", GetStringOrDefault("_STORE_CS", null)))?.Trim();
                    if (string.IsNullOrEmpty(cs)) {
                        return null;
                    }
                    return cs;
                }
                return "DefaultEndpointsProtocol=https;" +
                    $"EndpointSuffix={suffix};AccountName={account};AccountKey={key}";
            }
        }

        /// <summary> Checkpoint storage </summary>
        public string LeaseContainerName => GetStringOrDefault(kLeaseContainerNameKey, null);
        /// <summary> Receive batch size </summary>
        public int ReceiveBatchSize =>
            GetIntOrDefault(kReceiveBatchSizeKey, 999);
        /// <summary> Receive timeout </summary>
        public TimeSpan ReceiveTimeout =>
            GetDurationOrDefault(kReceiveTimeoutKey, TimeSpan.FromSeconds(5));

        /// <summary>
        /// Configuration constructor
        /// </summary>
        /// <param name="serviceId"></param>
        /// <param name="configuration"></param>
        public EventProcessorConfig(IConfigurationRoot configuration, string serviceId = "") :
            base(configuration) {
            _serviceId = serviceId ?? throw new ArgumentNullException(nameof(serviceId));
        }

        private readonly string _serviceId;
    }
}
