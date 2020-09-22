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
package com.dremio.datastore.format;

import java.util.UUID;

import com.dremio.datastore.Converter;
import com.dremio.datastore.DatastoreFatalException;
import com.dremio.datastore.FormatVisitor;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.format.compound.KeyPair;
import com.dremio.datastore.format.compound.KeyTriple;

import io.protostuff.Message;

/**
 * A FormatVisitor that produces serializers that convert to SER type.
 *
 * Overwriting the interface methods to have more specific types enables us to enforce type safety on the serializer
 * input and output for all serializer factories while also leaving the format visitor type generic.
 * @param <STORE> Storage type. Target type to serialize to.
 */
public interface SerializerFactory<STORE> extends FormatVisitor<Serializer<?, STORE>> {

  @Override
  @SuppressWarnings("unchecked")
  default <OUTER, INNER> Serializer<OUTER, STORE> visitWrappedFormat(Class<OUTER> clazz, Converter<OUTER, INNER> converter, Format<INNER> inner) throws DatastoreFatalException {
    return Serializer.wrap(converter, (Serializer<INNER, STORE>) inner.apply(this));
  }

  @Override
  Serializer<UUID, STORE> visitUUIDFormat() throws DatastoreFatalException;

  @Override
  Serializer<String, STORE> visitStringFormat() throws DatastoreFatalException;

  @Override
  <P extends Message<P>> Serializer<P, STORE> visitProtostuffFormat(Class<P> clazz) throws DatastoreFatalException;

  @Override
  <P extends com.google.protobuf.Message> Serializer<P, STORE> visitProtobufFormat(Class<P> clazz) throws DatastoreFatalException;

  @Override
  Serializer<byte[], STORE> visitByteFormat() throws DatastoreFatalException;

  @Override
  <K1, K2> Serializer<KeyPair<K1, K2>, STORE> visitCompoundPairFormat(
    String key1Name,
    Format<K1> key1Format,
    String key2Name,
    Format<K2> key2Format);

  @Override
  <K1, K2, K3> Serializer<KeyTriple<K1, K2, K3>, STORE> visitCompoundTripleFormat(
    String key1Name,
    Format<K1> key1Format,
    String key2Name,
    Format<K2> key2Format,
    String key3Name,
    Format<K3> key3Format) throws DatastoreFatalException;
}
