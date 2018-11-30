// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json.Linq;

    public static class DocumentEx {

        /// <summary>
        /// Convert document to json object
        /// </summary>
        /// <param name="doc"></param>
        /// <returns></returns>
        public static JObject ToJObject(this Document doc) {
            return (JObject)((dynamic)doc);
        }
    }
}
