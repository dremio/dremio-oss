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
package com.dremio.datastore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

import com.dremio.common.utils.UUIDAdapter;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.format.SerializerFactory;
import com.dremio.datastore.format.compound.KeyPair;
import com.dremio.datastore.format.compound.KeyTriple;
import com.google.common.base.Preconditions;
import com.google.protobuf.Parser;

import io.protostuff.Schema;

/**
 * Translates Generic Formats into serializers that translates data into bytes.
 * Used by both clients and servers. Servers, at the very least, need the serializers for backup.
 */
public final class ByteSerializerFactory implements SerializerFactory<byte[]> {

  public static final ByteSerializerFactory INSTANCE = new ByteSerializerFactory();

  private ByteSerializerFactory() {

  }

  @Override
  public Serializer<String, byte[]> visitStringFormat() {
    return StringSerializer.INSTANCE;
  }

  private static final class UUIDSerializer extends Serializer<UUID, byte[]> {

    public static final UUIDSerializer INSTANCE = new UUIDSerializer();

    private UUIDSerializer() {

    }

    @Override
    public byte[] convert(UUID v) {
      return UUIDAdapter.getBytesFromUUID(v);
    }


    @Override
    public UUID revert(byte[] v) {
      return UUIDAdapter.getUUIDFromBytes(v);
    }

    @Override
    public String toJson(UUID v) {
      return v.toString();
    }

    @Override
    public UUID fromJson(String v) {
      return UUID.fromString(v);
    }
  }

  @Override
  public Serializer<UUID, byte[]> visitUUIDFormat() {
    return UUIDSerializer.INSTANCE;
  }

  @Override
  public Serializer<byte[], byte[]> visitByteFormat() {
    return PassThroughSerializer.instance();
  }

  @Override
  public <K1, K2> Serializer<KeyPair<K1, K2>, byte[]> visitCompoundPairFormat(
    String key1Name,
    Format<K1> key1Format,
    String key2Name,
    Format<K2> key2Format) {
    throw new UnsupportedOperationException("Compound keys are not supported in ByteSerializerFactory");
  }

  @Override
  public <K1, K2, K3> Serializer<KeyTriple<K1, K2, K3>, byte[]> visitCompoundTripleFormat(
    String key1Name,
    Format<K1> key1Format,
    String key2Name,
    Format<K2> key2Format,
    String key3Name,
    Format<K3> key3Format) throws DatastoreFatalException {
    throw new UnsupportedOperationException("Compound keys are not supported in ByteSerializerFactory");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <P extends com.google.protobuf.Message> Serializer<P, byte[]> visitProtobufFormat(Class<P> clazz) {

    Parser<P> parser;
    try {
      Method defaultInstanceGetter = clazz.getDeclaredMethod("getDefaultInstance");
      com.google.protobuf.Message defaultInst = (com.google.protobuf.Message) defaultInstanceGetter.invoke(null);
      parser = (Parser<P>) defaultInst.getParserForType();
      Preconditions.checkNotNull(parser);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new DatastoreFatalException("Unable to get the parser for " + clazz.getName(), e);
    }

    return new ProtobufSerializer<>(clazz, parser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <P extends io.protostuff.Message<P>> Serializer<P, byte[]> visitProtostuffFormat(Class<P> clazz) {
    // getSchema is a method on all protostuff gen'ed classes.
    Schema<P> schema;
    try {
      Method schemaGetter = clazz.getDeclaredMethod("getSchema");
      schema = (Schema<P>) schemaGetter.invoke(null);
      assert schema != null;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new DatastoreFatalException("Unable to get Schema for Protostuff class " + clazz.getName(), e);
    }
    return new ProtostuffSerializer<>(schema);
  }
}
