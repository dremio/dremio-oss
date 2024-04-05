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

import com.dremio.datastore.Converter;
import com.dremio.datastore.DatastoreFatalException;
import com.dremio.datastore.FormatVisitor;
import com.dremio.datastore.format.compound.KeyPair;
import com.dremio.datastore.format.compound.KeyTriple;
import com.google.protobuf.Message;
import java.util.function.Function;

/**
 * !DO NOT CREATE NEW IMPLEMENTATIONS OF THIS INTERFACE! CONSIDER IT SEALED.
 *
 * <p>Used by storeCreationFunctions to indicate which supported format the kv store implementation
 * should use when serializing the class.
 *
 * @param <T> - The type of the key or value.
 */
public interface Format<T> {

  /** Get the class the format is the representation of. */
  Class<T> getRepresentedClass();

  /**
   * FormatVisitors decide the return type. Format is responsible for differentiating the type for
   * the visitor.
   *
   * @param visitor - Converts formats into RETs.
   * @return a serializer.
   * @throws DatastoreFatalException - when reflection fails to find the necessary resources.
   */
  <RET> RET apply(FormatVisitor<RET> visitor) throws DatastoreFatalException;

  /**
   * Create a Protobuf format from a Protobuf subclass.
   *
   * @param t the protobuf subclass.
   * @param <T> the protobuf subtype.
   * @return ProtobufFormat
   */
  static <T extends Message> Format<T> ofProtobuf(Class<T> t) {
    return new ProtobufFormat<>(t);
  }

  /**
   * Create a Protostuff format from a protostuff subclass.
   *
   * @param t the protostuff subclass
   * @param <T> the protobuf subtype.
   * @return ProtostuffFormat
   */
  static <T extends io.protostuff.Message> Format<T> ofProtostuff(Class<T> t) {
    return new ProtostuffFormat<>(t);
  }

  /**
   * Create a CompoundFormat from the provided arguments describing the keys.
   *
   * @param <K1> the type of the first key.
   * @param <K2> the type of for the second key.
   * @param key1Name the name of the first key.
   * @param key1Format the format subtype of the first key.
   * @param key2Name the name of the second key.
   * @param key2Format the format subtype of the second key.
   * @return CompoundFormat instance.
   */
  static <K1, K2> Format<KeyPair<K1, K2>> ofCompoundFormat(
      String key1Name, Format<K1> key1Format, String key2Name, Format<K2> key2Format) {
    return new CompoundPairFormat<>(key1Name, key1Format, key2Name, key2Format);
  }

  /**
   * Create a CompoundFormat from the provided arguments describing the keys.
   *
   * @param <K1> the type of the first key.
   * @param <K2> the type of for the second key.
   * @param <K3> the type of for the third key.
   * @param key1Name the name of the first key.
   * @param key1Format the format subtype of the first key.
   * @param key2Name the name of the second key.
   * @param key2Format the format subtype of the second key.
   * @param key3Name the name of the third key.
   * @param key3Format the format subtype of the third key.
   * @return CompoundFormat instance.
   */
  static <K1, K2, K3> Format<KeyTriple<K1, K2, K3>> ofCompoundFormat(
      String key1Name,
      Format<K1> key1Format,
      String key2Name,
      Format<K2> key2Format,
      String key3Name,
      Format<K3> key3Format) {
    return new CompoundTripleFormat<>(
        key1Name, key1Format, key2Name, key2Format, key3Name, key3Format);
  }

  /**
   * Creates a wrapped format that is actually a nestedFormat underneath.
   *
   * @param converter - Converts the wrapped type to the nested type.
   * @param nestedFormat - The format of the nested type.
   * @param <IN> - The input type.
   * @param <NESTED> - The nested type that the input type is converted into.
   * @return a Format for a converted type.
   */
  static <IN, NESTED> Format<IN> wrapped(
      Class<IN> in, Converter<IN, NESTED> converter, Format<NESTED> nestedFormat) {
    return new WrappedFormat<>(in, nestedFormat, converter);
  }

  /**
   * Sometimes it would be verbose to specify a full converter because the functions to convert are
   * already at hand.
   *
   * @param in The Class representation of IN. IN must be a type that does a deep comparison with
   *     equals().
   * @param to converts IN to NESTED
   * @param from converts NESTED to IN
   * @param nestedFormat format of the nested type
   * @param <IN> input type
   * @param <NESTED> nested type.
   * @return wrapped format.
   */
  static <IN, NESTED> Format<IN> wrapped(
      Class<IN> in,
      Function<IN, NESTED> to,
      Function<NESTED, IN> from,
      Format<NESTED> nestedFormat) {
    final Converter<IN, NESTED> c =
        new Converter<IN, NESTED>() {
          @Override
          public NESTED convert(IN v) {
            return to.apply(v);
          }

          @Override
          public IN revert(NESTED v) {
            return from.apply(v);
          }
        };

    return new WrappedFormat<>(in, nestedFormat, c);
  }

  /**
   * gets an instance of StringFormat
   *
   * @return StringFormat instance.
   */
  static StringFormat ofString() {
    return StringFormat.getInstance();
  }

  /**
   * gets an instance of UUIDFormat
   *
   * @return UUIDFormat instance.
   */
  static UUIDFormat ofUUID() {
    return UUIDFormat.getInstance();
  }

  /**
   * gets an instance of BytesFormat
   *
   * @return BytesFormat instance.
   */
  static BytesFormat ofBytes() {
    return BytesFormat.getInstance();
  }

  /** These are the types of formats that can be saved. */
  enum Types {
    BYTES,
    UUID,
    STRING,
    PROTOBUF,
    PROTOSTUFF,
    COMPOUND
  }
}
