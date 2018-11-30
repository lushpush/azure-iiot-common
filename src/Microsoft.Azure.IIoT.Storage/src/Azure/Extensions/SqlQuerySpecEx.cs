// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using Microsoft.Azure.Documents;
    using System;
    using System.Collections.Generic;
    using System.Text;

    public static class SqlQuerySpecEx {

        /// <summary>
        /// Add an array of parameters to a SqlQuerySpec to use with IN.
        /// </summary>
        /// <param name="spec">The spec to add parameters to.</param>
        /// <param name="array">The values that need to be added</param>
        /// <param name="name">The name of the param.</param>
        public static void AddArrayParameters(this SqlQuerySpec spec, 
            string prologue, string[] array, string name, string epilogue) {
            var parameters = new string[array.Length];
            for (var i = 0; i < array.Length; i++) {
                parameters[i] = $"@{name}{i.ToString("D4")}";
                spec.Parameters.Add(new SqlParameter(parameters[i], array[i]));
            }
            spec.QueryText += (prologue + string.Join(", ", parameters) + epilogue);
        }

        /// <summary>
        /// Convert to sql string for testing
        /// </summary>
        /// <param name="spec"></param>
        public static string ToRawSql(this SqlQuerySpec spec) {
            var sqlString = spec.QueryText;
            foreach(var param in spec.Parameters) {
                var value = param.Value.ToString();
                if (param.Value is string s) {
                    value = $"\"{s}\"";
                }
                sqlString = sqlString.Replace(param.Name, value);
            }
            return sqlString;
        }
    }
}
