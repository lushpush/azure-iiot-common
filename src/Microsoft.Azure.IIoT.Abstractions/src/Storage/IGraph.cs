// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage {

    /// <summary>
    /// Graph interface
    /// </summary>
    public interface IGraph {

        /// <summary>
        /// Open gremlin client to collection
        /// </summary>
        /// <exception cref="System.NotSupportedException"/>
        /// <returns></returns>
        IGremlinClient OpenGremlinClient();
    }
}
