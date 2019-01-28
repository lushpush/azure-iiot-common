// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace System.Xml {

    /// <summary>
    /// Helper extensions for qualified name
    /// </summary>
    public static class XmlQualifiedNameEx {

        /// <summary>
        /// Returns whether the name is null or empty string
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        public static bool IsNullOrEmpty(this XmlQualifiedName qn) =>
            qn == null || string.IsNullOrEmpty(qn.Name);
    }
}
