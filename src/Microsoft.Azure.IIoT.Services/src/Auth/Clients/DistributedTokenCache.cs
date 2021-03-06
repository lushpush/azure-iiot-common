// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Services.Auth.Clients {
    using Microsoft.Azure.IIoT.Auth.Clients;
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Extensions.Caching.Distributed;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.AspNetCore.DataProtection;
    using System;

    /// <summary>
    /// Token cache persisted in the distributed cache.
    /// </summary>
    public class DistributedTokenCache : ITokenCacheProvider {

        /// <summary>
        /// Create token store in provided distributed cache
        /// </summary>
        /// <param name="cache">Cache</param>
        /// <param name="dp">protector</param>
        /// <param name="logger"></param>
        public DistributedTokenCache(IDistributedCache cache,
            IDataProtectionProvider dp, ILogger logger) {
            _dp = dp ?? throw new ArgumentNullException(nameof(dp));
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public TokenCache GetCache(string name) =>
            new DistributedTokenCacheEntry(this, name);

        /// <summary>
        /// Cache implementation
        /// </summary>
        private class DistributedTokenCacheEntry : TokenCache {

            /// <summary>
            /// Create token cache entry in provided distributed cache
            /// </summary>
            /// <param name="cacheKey">Key in cache</param>
            /// <param name="store">cache to create entry in</param>
            public DistributedTokenCacheEntry(DistributedTokenCache store,
                string cacheKey) {
                var protector = store._dp.CreateProtector(GetType().FullName);

                AfterAccess = args => {
                    if (HasStateChanged) {
                        if (Count > 0) {
                            // Write our new token cache state to the cache
                            store._cache.Set(cacheKey, protector.Protect(Serialize()));
                        }
                        else {
                            // The Token cache is empty so remove ourselves.
                            store._cache.Remove(cacheKey);
                        }
                        HasStateChanged = false;
                    }
                };

                var cacheData = store._cache.Get(cacheKey);
                if (cacheData != null) {
                    Deserialize(protector.Unprotect(cacheData));
                }
            }
        }

        private readonly IDataProtectionProvider _dp;
        private readonly IDistributedCache _cache;
        private readonly ILogger _logger;
    }
}
