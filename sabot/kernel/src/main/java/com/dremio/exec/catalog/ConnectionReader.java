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
package com.dremio.exec.catalog;

import java.lang.reflect.Modifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.dremio.service.namespace.AbstractConnectionReader;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * Resolves concrete ConnectionConf types using Classpath Scanning.
 */
public class ConnectionReader implements AbstractConnectionReader {

  private static final Logger logger = LoggerFactory.getLogger(ConnectionReader.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private final ImmutableMap<String, Schema<ConnectionConf<?, ?>>> schemaByName;

  @SuppressWarnings("unchecked")
  public ConnectionReader(ScanResult scanResult) {
    ImmutableMap.Builder<String, Schema<ConnectionConf<?, ?>>> stringMap = ImmutableMap.builder();
    for(Class<?> input : scanResult.getAnnotatedClasses(SourceType.class)) {
      try {
        if(Modifier.isAbstract(input.getModifiers())
          || Modifier.isInterface(input.getModifiers())
          || !ConnectionConf.class.isAssignableFrom(input)) {
          logger.warn("Failure trying to recognize SourceConf for {}. Expected a concrete implementation of SourceConf.", input.getName());
          continue;
        }
      } catch (Exception e) {
        logger.warn("Failure trying to recognize SourceConf for {}", input.getName(), e);
        continue;
      }

      SourceType type = input.getAnnotation(SourceType.class);
      try {
        Schema<ConnectionConf<?, ?>> schema = (Schema<ConnectionConf<?, ?>>) RuntimeSchema.getSchema(input);
        stringMap.put(type.value(), schema);
      } catch(Exception ex) {
        throw new RuntimeException("failure trying to read source conf: " + input.getName(), ex);
      }
    }

    schemaByName = stringMap.build();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends AbstractConnectionConf> T getConnectionConf(String typeName, ByteString bytesS) {
    Schema<ConnectionConf<?, ?>> schema = schemaByName.get(typeName);
    if(schema == null) {
      throw new IllegalStateException(String.format("Unable to find handler for source of type [%s].", typeName));
    }

    ConnectionConf<?, ?> conf = schema.newMessage();
    byte[] bytes = bytesS.toByteArray();
    ProtobufIOUtil.mergeFrom(bytes, conf, schema);
    return (T) conf;
  }

  public ConnectionConf<?, ?> getConnectionConf(SourceConfig config) {
    Schema<ConnectionConf<?, ?>> schema = toSchema(config);
    ConnectionConf<?, ?> conf = schema.newMessage();
    byte[] bytes = config.getConfig().toByteArray();
    ProtobufIOUtil.mergeFrom(bytes, conf, schema);
    return conf;
  }

  private Schema<ConnectionConf<?, ?>> toSchema(SourceConfig config) {
    String typeName = toType(config);
    Schema<ConnectionConf<?, ?>> schema = schemaByName.get(typeName);
    if(schema != null) {
      return schema;
    }

    throw new IllegalStateException(String.format("Unable to find handler for source of type [%s].", typeName));
  }

  /**
   * Returns the given source config as a string, without secret fields. Useful in error messages and debug logs.
   *
   * @param sourceConfig source config
   * @return source config as string, without secret fields
   */
  String toStringWithoutSecrets(SourceConfig sourceConfig) {
    try {
      final byte[] bytes = ProtostuffIOUtil.toByteArray(sourceConfig, SourceConfig.getSchema(),
          LinkedBuffer.allocate());
      final SourceConfig clone = new SourceConfig();
      ProtostuffIOUtil.mergeFrom(bytes, clone, SourceConfig.getSchema());

      final ConnectionConf<?, ?> conf = getConnectionConf(clone);
      conf.clearSecrets();
      clone.setConfig(null);

      final StringBuilder sb = new StringBuilder();
      sb.append("[source: ")
          .append(clone.toString())
          .append(", connection: ");
      try {
        sb.append(mapper.writeValueAsString(conf));
      } catch (JsonProcessingException ignored) {
        sb.append("<serialization_error>");
      }
      sb.append("]");

      return sb.toString();
    } catch (Exception e) {
      return "failed to serialize: " + e.getMessage();
    }
  }

  @SuppressWarnings("deprecation")
  public static String toType(SourceConfig config) {
    if(config.getType() != null) {
      return config.getType();
    }

    if(config.getLegacySourceTypeEnum() != null) {
      return config.getLegacySourceTypeEnum().name();
    }

    throw new IllegalStateException(String.format("Unable to manage source of type: named: [%s], legacy enum: [%d].", config.getType(), config.getLegacySourceTypeEnum()));
  }


}
