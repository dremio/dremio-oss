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

import com.dremio.datastore.format.Format;

/**
 * Translate formats into rets. There are 6 supported formats - String - UUID - Byte - Protobuf -
 * Protostuff - CompoundPair - CompoundTriple - Wrapped (a format that converts a type into one of
 * the above formats.
 *
 * @param <RET> The return type.
 */
public interface FormatVisitor<RET> {

  RET visitStringFormat() throws DatastoreFatalException;

  RET visitUUIDFormat() throws DatastoreFatalException;

  RET visitByteFormat() throws DatastoreFatalException;

  /**
   * Visits a compound format with a KeyPair.
   *
   * @param <K1> - The type of the first key.
   * @param <K2> - The type of the second key.
   * @param key1Name the name of the first key.
   * @param key1Format the format subtype of the first key.
   * @param key2Name the name of the second key.
   * @param key2Format the format subtype of the second key.
   */
  <K1, K2> RET visitCompoundPairFormat(
      String key1Name, Format<K1> key1Format, String key2Name, Format<K2> key2Format);

  /**
   * Visits a compound format with a KeyTriple.
   *
   * @param <K1> - The type of the first key.
   * @param <K2> - The type of the second key.
   * @param <K3> - The type of the third key.
   * @param key1Name the name of the first key.
   * @param key1Format the format subtype of the first key.
   * @param key2Name the name of the second key.
   * @param key2Format the format subtype of the second key.
   * @param key3Name the name of the third key.
   * @param key3Format the format subtype of the third key.
   */
  <K1, K2, K3> RET visitCompoundTripleFormat(
      String key1Name,
      Format<K1> key1Format,
      String key2Name,
      Format<K2> key2Format,
      String key3Name,
      Format<K3> key3Format)
      throws DatastoreFatalException;

  /**
   * Visits a wrapped format
   *
   * @param <OUTER> the outside type. It is visible to the user (equivalent to Format<OUTER>)
   * @param <INNER> the inside type. Not visible to the user
   * @param clazz the class of the object represented by this format (equivalent to Class<OUTER>)
   * @param converter the converter between outer and inner type
   * @param inner the inner format
   * @return
   * @throws DatastoreFatalException
   */
  <OUTER, INNER> RET visitWrappedFormat(
      Class<OUTER> clazz, Converter<OUTER, INNER> converter, Format<INNER> inner)
      throws DatastoreFatalException;

  <P extends com.google.protobuf.Message> RET visitProtobufFormat(Class<P> clazz)
      throws DatastoreFatalException;

  <P extends io.protostuff.Message<P>> RET visitProtostuffFormat(Class<P> clazz)
      throws DatastoreFatalException;
}
