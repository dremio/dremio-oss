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

import java.util.Arrays;

import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.format.Format;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;

/**
 * Container that stringifies store to config
 * and can get instances of classes.
 * @param <K> key
 * @param <V> value
 */
@Deprecated
public final class LegacyStoreBuilderHelper<K, V> {

  private final KVStoreInfo info = new KVStoreInfo();
  private Format<K> keyFormat;
  private Format<V> valueFormat;
  private Class<? extends VersionExtractor<V>> versionExtractorClass;
  private DocumentConverter<K, V> documentConverter;

  public LegacyStoreBuilderHelper() {
    info.setVersionExtractorClassName("");
  }

  public LegacyStoreBuilderHelper<K, V> name(String name) {
    info.setTablename(name);
    return this;
  }

  public LegacyStoreBuilderHelper<K, V> keyFormat(Format<K> keyFormat) {
    Preconditions.checkNotNull(keyFormat);
    this.keyFormat = keyFormat;
    this.info.setKeyFormat(FormatInfoFactory.of(keyFormat));
    return this;
  }

  public LegacyStoreBuilderHelper<K, V> valueFormat(Format<V> valueFormat) {
    Preconditions.checkNotNull(valueFormat);
    this.valueFormat = valueFormat;
    this.info.setValueFormat(FormatInfoFactory.of(valueFormat));
    return this;
  }

  public LegacyStoreBuilderHelper<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass) {
    Preconditions.checkNotNull(versionExtractorClass);
    this.versionExtractorClass = versionExtractorClass;
    info.setVersionExtractorClassName(versionExtractorClass.getName());
    return this;
  }

  public LegacyStoreBuilderHelper<K, V> documentConverter(DocumentConverter<K, V> documentConverter) {
    Preconditions.checkNotNull(documentConverter);
    this.documentConverter = documentConverter;
    return this;
  }

  public boolean hasDocumentConverter() {
    return this.documentConverter != null;
  }

  public String getName() {
    return info.getTablename();
  }

  @SuppressWarnings("unchecked")
  public Serializer<K, byte[]> getKeyByteSerializer() {
    return (Serializer<K, byte[]>) keyFormat.apply(ByteSerializerFactory.INSTANCE);
  }

  @SuppressWarnings("unchecked")
  public Serializer<V, byte[]> getValueByteSerializer() {
    return (Serializer<V, byte[]>) valueFormat.apply(ByteSerializerFactory.INSTANCE);
  }

  public Format<K> getKeyFormat() {
    return keyFormat;
  }

  public Format<V> getValueFormat() {
    return valueFormat;
  }

  public DocumentConverter<K, V> getDocumentConverter() {
    return documentConverter;
  }

  public VersionExtractor<V> tryGetVersionExtractor() {
    return versionExtractorClass == null ? null : DataStoreUtils.getInstance(versionExtractorClass);
  }

  public KVStoreInfo getKVStoreInfo() {
    return info;
  }

  private static final class FormatInfoFactory implements FormatVisitor<KVFormatInfo> {

    private static FormatInfoFactory INSTANCE = new FormatInfoFactory();

    private static KVFormatInfo of(Format<?> format) {
      return format.apply(INSTANCE);
    }

    private static KVFormatInfo of(KVFormatInfo.Type type) {
      return new KVFormatInfo().setType(type);
    }

    private static KVFormatInfo of(KVFormatInfo.Type type, Class<?> clazz) {
      return of(type).setClassNameParam(clazz.getName());
    }

    @Override
    public KVFormatInfo visitByteFormat() throws DatastoreFatalException {
      return of(KVFormatInfo.Type.BYTES);
    }

    @Override
    public <K1, K2> KVFormatInfo visitCompoundPairFormat(
      String key1Name,
      Format<K1> key1Format,
      String key2Name,
      Format<K2> key2Format) {

      return of(KVFormatInfo.Type.COMPOUND)
        .setCompoundFieldsList(
          Arrays.asList(
            key1Format.apply(this),
            key2Format.apply(this)));
    }

    @Override
    public <K1, K2, K3> KVFormatInfo visitCompoundTripleFormat(
      String key1Name,
      Format<K1> key1Format,
      String key2Name,
      Format<K2> key2Format,
      String key3Name,
      Format<K3> key3Format) throws DatastoreFatalException {

      return of(KVFormatInfo.Type.COMPOUND)
        .setCompoundFieldsList(
          Arrays.asList(
            key1Format.apply(this),
            key2Format.apply(this),
            key3Format.apply(this)));
    }

    @Override
    public <P extends Message> KVFormatInfo visitProtobufFormat(Class<P> clazz) throws DatastoreFatalException {
      return of(KVFormatInfo.Type.PROTOBUF, clazz);
    }

    @Override
    public <P extends io.protostuff.Message<P>> KVFormatInfo visitProtostuffFormat(Class<P> clazz) throws DatastoreFatalException {
      return of(KVFormatInfo.Type.PROTOSTUFF, clazz);
    }

    @Override
    public KVFormatInfo visitStringFormat() throws DatastoreFatalException {
      return of(KVFormatInfo.Type.STRING);
    }

    @Override
    public KVFormatInfo visitUUIDFormat() throws DatastoreFatalException {
      return of(KVFormatInfo.Type.UUID);
    }

    @Override
    public <OUTER, INNER> KVFormatInfo visitWrappedFormat(Class<OUTER> clazz, Converter<OUTER, INNER> converter, Format<INNER> inner) throws DatastoreFatalException {
      return inner.apply(this);
    }
  }
}
