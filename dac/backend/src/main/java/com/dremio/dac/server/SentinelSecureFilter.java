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
package com.dremio.dac.server;

import com.dremio.common.SentinelSecure;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyFilter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;

/** A filter that automatically replaces a value with a Sentinel value if being serialized. */
public final class SentinelSecureFilter implements PropertyFilter {

  public static final PropertyFilter SECURE = new SentinelSecureFilter(true);
  public static final PropertyFilter TEST_ONLY = new SentinelSecureFilter(false);

  private boolean transform;

  private SentinelSecureFilter(boolean transform) {
    super();
    this.transform = transform;
  }

  @Override
  public void serializeAsField(
      Object pojo, JsonGenerator gen, SerializerProvider prov, PropertyWriter writer)
      throws Exception {
    filter(writer).serializeAsField(pojo, gen, prov);
  }

  @Override
  public void serializeAsElement(
      Object elementValue, JsonGenerator gen, SerializerProvider prov, PropertyWriter writer)
      throws Exception {
    filter(writer).serializeAsElement(elementValue, gen, prov);
  }

  private PropertyWriter filter(PropertyWriter writer) {
    if (!transform) {
      return writer;
    }

    SentinelSecure secure = writer.getAnnotation(SentinelSecure.class);
    if (secure == null || secure.value() == null || secure.value().isEmpty()) {
      return writer;
    }

    return new SensitivePropertyWriter((BeanPropertyWriter) writer, secure.value());
  }

  @SuppressWarnings("deprecation")
  @Override
  public void depositSchemaProperty(
      PropertyWriter writer, ObjectNode propertiesNode, SerializerProvider provider)
      throws JsonMappingException {
    writer.depositSchemaProperty(propertiesNode, provider);
  }

  @Override
  public void depositSchemaProperty(
      PropertyWriter writer, JsonObjectFormatVisitor objectVisitor, SerializerProvider provider)
      throws JsonMappingException {
    writer.depositSchemaProperty(objectVisitor, provider);
  }

  private static class SensitivePropertyWriter extends BeanPropertyWriter {
    private final BeanPropertyWriter writer;
    private final String replacementValue;

    public SensitivePropertyWriter(BeanPropertyWriter writer, String replacementValue) {
      super();
      this.writer = writer;
      this.replacementValue = replacementValue;
    }

    @Override
    public void serializeAsField(Object bean, JsonGenerator gen, SerializerProvider prov)
        throws Exception {
      Object value = writer.get(bean);
      if (value == null) {
        writer.serializeAsField(bean, gen, prov);
        return;
      }

      if (!(value instanceof String)) {
        throw new UnsupportedOperationException(
            "SentinelSecure is only allowed on String properties.");
      }

      String str = (String) value;
      if (!str.isEmpty()) {
        gen.writeStringField(writer.getName(), replacementValue);
      } else {
        writer.serializeAsField(bean, gen, prov);
      }
    }

    @Override
    public void serializeAsElement(Object bean, JsonGenerator gen, SerializerProvider prov)
        throws Exception {
      throw new UnsupportedOperationException(
          "SentinelSecure is only allowed on String properties, not Lists or Arrays.");
    }
  }
}
