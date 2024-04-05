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
import java.util.Arrays;
import java.util.UUID;

/** A simple FormatVisitor implementation to use for testing Format in the visitor. */
public class FormatVisitorTestImpl implements FormatVisitor<FormatVisitorTestImpl.Value<?>> {
  public static final FormatVisitorTestImpl INSTANCE = new FormatVisitorTestImpl();

  @Override
  public Value<String> visitStringFormat() throws DatastoreFatalException {
    return Value.ofString();
  }

  @Override
  public Value<UUID> visitUUIDFormat() throws DatastoreFatalException {
    return Value.ofUUID();
  }

  @Override
  public Value<byte[]> visitByteFormat() throws DatastoreFatalException {
    return Value.ofBytes();
  }

  @Override
  public <K1, K2> Value<KeyPair<Value<K1>, Value<K2>>> visitCompoundPairFormat(
      String key1Name, Format<K1> key1Format, String key2Name, Format<K2> key2Format) {
    return Value.<K1, K2>ofKeyPair(
        key1Name, (Value<K1>) key1Format.apply(this), key2Name, (Value<K2>) key2Format.apply(this));
  }

  @Override
  public <K1, K2, K3> Value<KeyTriple<Value<K1>, Value<K2>, Value<K3>>> visitCompoundTripleFormat(
      String key1Name,
      Format<K1> key1Format,
      String key2Name,
      Format<K2> key2Format,
      String key3Name,
      Format<K3> key3Format)
      throws DatastoreFatalException {
    return Value.<K1, K2, K3>ofKeyTriple(
        key1Name,
        (Value<K1>) key1Format.apply(this),
        key2Name,
        (Value<K2>) key2Format.apply(this),
        key3Name,
        (Value<K3>) key3Format.apply(this));
  }

  @Override
  public <OUTER, INNER> Value<OUTER> visitWrappedFormat(
      Class<OUTER> clazz, Converter<OUTER, INNER> converter, Format<INNER> inner)
      throws DatastoreFatalException {
    return Value.ofWrapped(clazz, converter, (Value<INNER>) inner.apply(this));
  }

  @Override
  public <P extends Message> Value<P> visitProtobufFormat(Class<P> clazz)
      throws DatastoreFatalException {
    return Value.ofProtobuf(clazz);
  }

  @Override
  public <P extends io.protostuff.Message<P>> Value<P> visitProtostuffFormat(Class<P> clazz)
      throws DatastoreFatalException {
    return Value.ofProtostuff(clazz);
  }

  /**
   * Test Visitable that captures state of each Visitor call.
   *
   * @param <T> The type passed to the visitor.
   */
  public static class Value<T> {
    private T whatAmI;
    private Class<T> whatClassAmI;
    private String key1Name = null;
    private String key2Name = null;
    private String key3Name = null;

    private Value(T whatAmI) {
      this.whatAmI = whatAmI;
      this.whatClassAmI = (Class<T>) whatAmI.getClass();
    }

    private Value(T whatAmI, String key1Name, String key2Name) {
      this(whatAmI);
      this.key1Name = key1Name;
      this.key2Name = key2Name;
    }

    private Value(T whatAmI, String key1Name, String key2Name, String key3Name) {
      this(whatAmI, key1Name, key2Name);
      this.key3Name = key3Name;
    }

    public Value(Class<T> clazz) {
      whatClassAmI = clazz;
    }

    @Override
    public String toString() {
      return "{ class = "
          + whatClassAmI.getName()
          + ", "
          + (whatAmI != null
              ? (whatAmI instanceof byte[] ? Arrays.toString((byte[]) whatAmI) : whatAmI.toString())
              : "no value")
          + ", key1name = "
          + key1Name
          + ", key2name = "
          + key2Name
          + ", key3name = "
          + key3Name
          + " } ";
    }

    public static Value<String> ofString() {
      return new Value<>("I am a string");
    }

    public static Value<UUID> ofUUID() {
      return new Value<>(UUID.fromString("79b4881b-9ea3-45a8-bae1-63c274b4a90b"));
    }

    public static Value<byte[]> ofBytes() {
      return new Value<>(new byte[] {0, 1, 2, 3, 4, 5, 0});
    }

    public static <K1, K2> Value<KeyPair<Value<K1>, Value<K2>>> ofKeyPair(
        String key1Name, Value<K1> key1, String key2Name, Value<K2> key2) {
      return new Value<>(new KeyPair<>(key1, key2), key1Name, key2Name);
    }

    public static <K1, K2, K3> Value<KeyTriple<Value<K1>, Value<K2>, Value<K3>>> ofKeyTriple(
        String key1Name,
        Value<K1> key1,
        String key2Name,
        Value<K2> key2,
        String key3Name,
        Value<K3> key3) {
      return new Value<>(new KeyTriple<>(key1, key2, key3), key1Name, key2Name, key3Name);
    }

    public static <OUTER, INNER> Value<OUTER> ofWrapped(
        Class<OUTER> outerClazz, Converter<OUTER, INNER> converter, Value<INNER> innerValue) {
      return new Value<>((OUTER) new WrappedValue<>(outerClazz, converter, innerValue));
    }

    public static <P extends Message> Value<P> ofProtobuf(Class<P> clazz) {
      return new Value<>(clazz);
    }

    public static <P extends io.protostuff.Message<P>> Value<P> ofProtostuff(Class<P> clazz) {
      return new Value<>(clazz);
    }

    private static class WrappedValue<INNER, OUTER> extends Value<OUTER> {
      private final Class<OUTER> outerClazz;
      private final Converter<OUTER, INNER> converter;
      private final Value<INNER> innerValue;

      public WrappedValue(
          Class<OUTER> outerClazz, Converter<OUTER, INNER> converter, Value<INNER> innerValue) {
        super(outerClazz);
        this.outerClazz = outerClazz;
        this.converter = converter;
        this.innerValue = innerValue;
      }

      @Override
      public String toString() {
        return "{ outerClass = "
            + outerClazz.getName()
            + ", "
            + " innerClass = "
            + innerValue.toString()
            + ", "
            + " converterClass = "
            + converter.getClass().getName()
            + "} ";
      }
    }
  }
}
