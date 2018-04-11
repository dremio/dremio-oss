/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.coordinator;

import java.io.IOException;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Serializer/Deserializer helper classes for {@code com.dremio.exec.proto.CoordinationProtos.NodeEndpoint}
 */
public class NodeEndpointSerDe {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NodeEndpointSerDe.class);

  /**
   * JSON deserializer for {@code com.dremio.exec.proto.CoordinationProtos.NodeEndpoint}
   */
  public static class De extends StdDeserializer<NodeEndpoint> {

    public De() {
      super(NodeEndpoint.class);
    }

    @Override
    public NodeEndpoint deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      return NodeEndpoint.parseFrom(jp.getBinaryValue());
    }


  }

  /**
   * JSON serializer for {@code com.dremio.exec.proto.CoordinationProtos.NodeEndpoint}
   */
  public static class Se extends StdSerializer<NodeEndpoint> {

    public Se() {
      super(NodeEndpoint.class);
    }

    @Override
    public void serialize(NodeEndpoint value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      jgen.writeBinary(value.toByteArray());
    }

  }
}
