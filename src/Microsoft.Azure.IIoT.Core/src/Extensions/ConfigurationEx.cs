// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Extensions.Configuration {
    using System.IO;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Extension methods
    /// </summary>
    public static class ConfigurationEx {

        /// <summary>
        /// Adds .env file environment variables
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static IConfigurationBuilder AddDotEnvFile(this IConfigurationBuilder builder) {
            var curDir = Environment.CurrentDirectory;
            try {
                while (!string.IsNullOrEmpty(curDir) && !File.Exists(Path.Combine(curDir, ".env"))) {
                    // Find .env file
                    curDir = Path.GetDirectoryName(Environment.CurrentDirectory);
                }
                if (!string.IsNullOrEmpty(curDir)) {
                    var lines = File.ReadAllLines(Path.Combine(curDir, ".env"));
                    var values = new Dictionary<string, string>();
                    foreach (var line in lines) {
                        var offset = line.IndexOf('=');
                        if (offset == -1) {
                            continue;
                        }
                        var key = line.Substring(0, offset).Trim();
                        if (key.StartsWith("#", StringComparison.Ordinal)) {
                            continue;
                        }
                        values.AddOrUpdate(key, line.Substring(offset + 1));
                    }
                    builder.AddInMemoryCollection(values);
                }
            }
            catch (IOException) { }
            return builder;
        }
    }
}
