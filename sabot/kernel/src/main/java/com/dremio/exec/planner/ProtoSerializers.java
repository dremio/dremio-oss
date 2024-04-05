/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.exec.planner;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.io.IOException;

@SuppressWarnings("serial")
public class ProtoSerializers {

  public static <X> void registerPojo(SimpleModule module, Class<X> clazz) {
    Schema<X> schema = RuntimeSchema.getSchema(clazz);
    module.addDeserializer(clazz, new ProtostufStdDeserializer<>(clazz, schema));
    module.addSerializer(clazz, new ProtostufStdSerializer<>(clazz, schema));
  }

  @SuppressWarnings("unchecked")
  public static <X> void registerSchema(SimpleModule module, Schema<X> schema) {
    Class<X> clazz = (Class<X>) schema.newMessage().getClass();
    module.addDeserializer(clazz, new ProtostufStdDeserializer<>(clazz, schema));
    module.addSerializer(clazz, new ProtostufStdSerializer<>(clazz, schema));
  }

  private static class ProtostufStdDeserializer<X> extends StdDeserializer<X> {
    private final Schema<X> schema;

    public ProtostufStdDeserializer(Class<X> clazz, Schema<X> schema) {
      super(clazz);
      this.schema = schema;
    }

    @Override
    public X deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      X msg = schema.newMessage();
      ProtostuffIOUtil.mergeFrom(p.getBinaryValue(), msg, schema);
      //      p.nextToken();
      //      JsonIOUtil.mergeFrom(p, msg, schema, false);
      return msg;
    }
  }

  private static class ProtostufStdSerializer<X> extends StdSerializer<X> {
    private final Schema<X> schema;

    public ProtostufStdSerializer(Class<X> clazz, Schema<X> schema) {
      super(clazz);
      this.schema = schema;
    }

    @Override
    public void serialize(X value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeBinary(ProtostuffIOUtil.toByteArray(value, schema, LinkedBuffer.allocate()));
      // JsonIOUtil.writeTo(gen, value, schema, false);
    }
  }
}
