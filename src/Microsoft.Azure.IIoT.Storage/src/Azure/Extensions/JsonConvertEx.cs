// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.Documents {
    using System;
    using Newtonsoft.Json.Linq;
    using Microsoft.Azure.IIoT.Utils;
    using System.Diagnostics;
    using System.Collections.Immutable;
    using System.Linq;

    /// <summary>
    /// Converts an statement to and from a cosmos db graph edge 
    /// </summary>
    public static class JsonConvertEx {

        /// <summary>
        /// Read statement directly from edge json
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static Statement ToStatement(this JObject json) {
            var ts = DateTime.FromBinary(json.Value<long>("_ts"));
            var created = ts;
            if (json.TryGetValue("created", out JToken cs)) {
                created = DateTime.FromBinary((long)cs);
            }
            if (json.TryGetValue("_isDeleted", out JToken d) ||
                !json.TryGetValue("id", out JToken id)) {
                return null;
            }

            // Read subject
            if (!json.TryGetValue("_vertexResource", out JToken _vertexResource)) {
                return null;
            }
            var s = ToResource(_vertexResource);
            // Read object
            if (!json.TryGetValue("_sinkResource", out JToken _sinkResource)) {
                return null;
            }
            var o = ToResource(_sinkResource);
            if (!json.TryGetValue("predicate", out JToken p) ||
                !json.TryGetValue("graphs", out JToken graphs)) {
                return null;
            }
            return new Statement(s, (string)p, o,
                new Uri(((JArray)graphs).Last.ToString())); // TODO
        }

        /// <summary>
        /// Converts statement to edge json
        /// </summary>
        /// <param name="statement"></param>
        /// <param name="graph"></param>
        /// <returns></returns>
        public static JObject ToJson(this Statement statement, Uri graph,
            int? docExpirationInSeconds) {

            var json = new JObject {
                new JProperty("id", statement.ToSha1Hash(graph)),
                new JProperty("label", statement.Predicate),
                new JProperty("predicate", statement.Predicate),
                new JProperty("graphs", new JArray(graph.AbsoluteUri)),

                // Attach sink/source info
                new JProperty("_isEdge", true),

                new JProperty("_vertexId", statement.Subject.Id),
                new JProperty("_vertexLabel", statement.Subject.ToString()),
                new JProperty("_vertexResource", ToJson(statement.Subject)),

                new JProperty("_sink", statement.Object.Id),
                new JProperty("_sinkLabel", statement.Object.ToString()),
                new JProperty("_sinkResource", ToJson(statement.Object))
            };

            if (docExpirationInSeconds.HasValue) {
                json.Add(new JProperty("ttl", docExpirationInSeconds.Value));
            }
            if (statement.Falseness.HasValue) {
                json.Add(new JProperty("falseness", statement.Falseness.Value));
            }
            if (statement.Created != DateTime.MinValue) {
                json.Add(new JProperty("created", statement.Created.ToBinary()));
            }
            return json;
        }

        /// <summary>
        /// Read resource from json object directly
        /// </summary>
        public static Resource ToResource(JToken resource) {
            if (resource.Type != JTokenType.Object) {
                return null;
            }
            var o = (JObject)resource;
            if (!o.TryGetValue("content", out var content)) {
                return null;
            }
            var _type = o.Value<string>("_type");
            switch (_type) {
                case nameof(Value):
                    o.TryGetValue("datatype", out var datatype);
                    o.TryGetValue("lang", out var lang);
                    return Value.CreateFromJson(content, (string)datatype, (string)lang);
                case nameof(Anonymous):
                    return Anonymous.Create(content);
                case nameof(List):
                    var array = (JArray)resource;
                    return List.Create(array.Select(i => ToResource(i)));
                case nameof(Named):
                    return Named.Create((Uri)content);
                default:
                    throw new FormatException($"Unexpected type {_type}");
            }
        }

        /// <summary>
        /// Returns a resource as vertex json
        /// </summary>
        /// <param name="resource"></param>
        /// <returns></returns>
        public static JObject ToJson(this Resource resource) {
            var o = new JObject();
            switch (resource) {
                case Value value:
                    o.Add(new JProperty("_type", nameof(Value)));
                    if (value.DataType != null) {
                        o.Add(new JProperty("datatype", value.DataType));
                    }
                    if (value.Language != null) {
                        o.Add(new JProperty("lang", value.Language));
                    }
                    o.Add(new JProperty("content", resource.Content));
                    break;
                case Anonymous blank:
                    o.Add(new JProperty("_type", nameof(Anonymous)));
                    o.Add(new JProperty("content", blank.Content));
                    break;
                case List list:
                    o.Add(new JProperty("_type", nameof(List)));
                    var array = new JArray();
                    foreach (var r in list) {
                        array.Add(ToJson(r));
                    }
                    o.Add(new JProperty("content", array));
                    break;
                case Named named:
                    o.Add(new JProperty("_type", nameof(Named)));
                    o.Add(new JProperty("content", named.Content));
                    break;
                default:
                    throw new InvalidOperationException("Unexpected");
            }
            return o;
        }


        /// <summary>
        /// Returns a resource as vertex json
        /// </summary>
        /// <param name="resource"></param>
        /// <returns></returns>
        public static JObject ToJson(this Resource resource,
            int? docExpirationInSeconds) {
            var o = ToJson(resource);
            if (docExpirationInSeconds != null) {
                if (docExpirationInSeconds.HasValue) {
                    o.Add(new JProperty("ttl", docExpirationInSeconds.Value));
                }
            }
            return o;
        }
    }
}
