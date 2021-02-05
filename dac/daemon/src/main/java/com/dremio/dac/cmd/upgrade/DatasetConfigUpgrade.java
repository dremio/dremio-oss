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

package com.dremio.dac.cmd.upgrade;

import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.OldField;
import org.apache.arrow.flatbuf.OldSchema;
import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.Version;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.flatbuffers.FlatBufferBuilder;

import io.protostuff.ByteString;

/**
 * To upgrade Arrow Binary Schema to latest Arrow release Dremio uses as of 2.1.0 release
 * Looks like we have 3 stores that store DatasetConfig that contains binary Schema
 */
public class DatasetConfigUpgrade extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "18df9bdf-9186-4780-b6bb-91bcb14a7a8b";

  public DatasetConfigUpgrade() {
    super("Upgrade Arrow Schema", ImmutableList.of());
  }


  @Override
  public Version getMaxVersion() {
    return VERSION_210;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final LegacyKVStoreProvider localStore = context.getKVStoreProvider();

    final LegacyKVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> vdsVersionStore =
      localStore.getStore(DatasetVersionMutator.VersionStoreCreator.class);

    final LegacyIndexedStore<String, NameSpaceContainer> namespaceStore =
      localStore.getStore(NamespaceServiceImpl.NamespaceStoreCreator.class);

    Iterable<Map.Entry<String, NameSpaceContainer>> nameSpaces = namespaceStore.find();
    StreamSupport.stream(nameSpaces.spliterator(), false)
      .filter(entry -> NameSpaceContainer.Type.DATASET == entry.getValue().getType())
      .forEach(entry -> {
        DatasetConfig datasetConfig = update(entry.getValue().getDataset());
        if (datasetConfig != null) {
          entry.getValue().setDataset(datasetConfig);
          namespaceStore.put(entry.getKey(), entry.getValue());
        }
      });

    Iterable<Map.Entry<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>> vdsEntries =
      vdsVersionStore.find();

    for (Map.Entry<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> vdsEntry : vdsEntries) {
      VirtualDatasetVersion vdv = vdsEntry.getValue();
      DatasetConfig datasetConfig = update(vdv.getDataset());
      if (datasetConfig == null) {
        continue;
      }
      vdv.setDataset(datasetConfig);
      vdsVersionStore.put(vdsEntry.getKey(), vdv);
    }
  }

  /**
   * Updates the Arrow Schema properties.
   *
   * @param datasetConfig the Arrow Schema properties
   * @return              the Arrow Schema properties updated
   */
  private DatasetConfig update(DatasetConfig datasetConfig) {
    if (datasetConfig == null) {
      return null;
    }
    final io.protostuff.ByteString schemaBytes = DatasetHelper.getSchemaBytes(datasetConfig);
    if (schemaBytes == null) {
      return null;
    }
    try {
      OldSchema oldSchema = OldSchema.getRootAsOldSchema(schemaBytes.asReadOnlyByteBuffer());
      byte[] newschemaBytes = convertFromOldSchema(oldSchema);
      datasetConfig.setRecordSchema(ByteString.copyFrom(newschemaBytes));
      return datasetConfig;
    } catch (Exception e) {
      System.out.println("Unable to update Arrow Schema for: " + PathUtils
        .constructFullPath(Optional.ofNullable(datasetConfig.getFullPathList()).orElse(Lists.newArrayList())));
      e.printStackTrace(System.out);
      return null;
    }
  }

  /**
   * Converts the old Arrow schema to new one based on Arrow version used in Dremio as of 2.1.0.
   *
   * @param oldSchema the old Arrow schema will be converted
   * @return          the new schema to Arrow format as of Dremio 2.1.0+, serialized by the FlatBuffer
   */
  @VisibleForTesting
  byte[] convertFromOldSchema(OldSchema oldSchema) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int[] fieldOffsets = new int[oldSchema.fieldsLength()];
    for (int i = 0; i < oldSchema.fieldsLength(); i++) {
      fieldOffsets[i] = convertFromOldField(oldSchema.fields(i), builder);
    }
    int fieldsOffset = org.apache.arrow.flatbuf.Schema.createFieldsVector(builder, fieldOffsets);
    int[] metadataOffsets = new int[oldSchema.customMetadataLength()];
    for (int i = 0; i < metadataOffsets.length; i++) {
      int keyOffset = builder.createString(oldSchema.customMetadata(i).key());
      int valueOffset = builder.createString(oldSchema.customMetadata(i).value());
      KeyValue.startKeyValue(builder);
      KeyValue.addKey(builder, keyOffset);
      KeyValue.addValue(builder, valueOffset);
      metadataOffsets[i] = KeyValue.endKeyValue(builder);
    }
    int metadataOffset = org.apache.arrow.flatbuf.Field.createCustomMetadataVector(builder, metadataOffsets);
    org.apache.arrow.flatbuf.Schema.startSchema(builder);
    org.apache.arrow.flatbuf.Schema.addFields(builder, fieldsOffset);
    org.apache.arrow.flatbuf.Schema.addCustomMetadata(builder, metadataOffset);
    builder.finish(org.apache.arrow.flatbuf.Schema.endSchema(builder));
    return builder.sizedByteArray();
  }

  /**
   * Converts the old Arrow Field to new one based on Arrow version used in Dremio as of 2.1.0.
   *
   * @param oldField the old field to be converted
   * @param builder  a FlatBufferBuilder to serialize the field
   * @return         the new field to Arrow format as of Dremio 2.1.0+, serialized by the FlatBuffer
   */
  private int convertFromOldField(OldField oldField, FlatBufferBuilder builder) {
    int nameOffset = oldField.name() == null ? -1 : builder.createString(oldField.name());
    ArrowType arrowType = getTypeForField(oldField);
    int typeOffset = arrowType.getType(builder);
    int dictionaryOffset = -1;
    org.apache.arrow.flatbuf.DictionaryEncoding oldDictionaryEncoding = oldField.dictionary();
    if (oldDictionaryEncoding != null) {
      int intType = Int.createInt(builder,
        oldDictionaryEncoding.indexType().bitWidth(),
        oldDictionaryEncoding.indexType().isSigned());

      dictionaryOffset =
        org.apache.arrow.flatbuf.DictionaryEncoding.createDictionaryEncoding(builder,
          oldDictionaryEncoding.id(),
          intType,
          oldDictionaryEncoding.isOrdered(),
          oldDictionaryEncoding.dictionaryKind());
    }
    int childrenLength = oldField.childrenLength();
    int[] childrenData = new int[childrenLength];
    for (int i = 0; i < childrenLength; i++) {
      childrenData[i] = convertFromOldField(oldField.children(i), builder);
    }
    int childrenOffset = org.apache.arrow.flatbuf.Field.createChildrenVector(builder, childrenData);
    int metadataLength = oldField.customMetadataLength();
    int[] metadataOffsets = new int[metadataLength];
    for (int i = 0; i < metadataOffsets.length; i++) {
      KeyValue keyValue = oldField.customMetadata(i);
      int keyOffset = builder.createString(keyValue.key());
      int valueOffset = builder.createString(keyValue.value());
      KeyValue.startKeyValue(builder);
      KeyValue.addKey(builder, keyOffset);
      KeyValue.addValue(builder, valueOffset);
      metadataOffsets[i] = KeyValue.endKeyValue(builder);
    }
    int metadataOffset = org.apache.arrow.flatbuf.Field.createCustomMetadataVector(builder, metadataOffsets);
    org.apache.arrow.flatbuf.Field.startField(builder);
    if (oldField.name() != null) {
      org.apache.arrow.flatbuf.Field.addName(builder, nameOffset);
    }
    org.apache.arrow.flatbuf.Field.addNullable(builder, oldField.nullable());
    org.apache.arrow.flatbuf.Field.addTypeType(builder, oldField.typeType());
    org.apache.arrow.flatbuf.Field.addType(builder, typeOffset);
    org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
    org.apache.arrow.flatbuf.Field.addCustomMetadata(builder, metadataOffset);
    if (oldDictionaryEncoding != null) {
      org.apache.arrow.flatbuf.Field.addDictionary(builder, dictionaryOffset);
    }
    return org.apache.arrow.flatbuf.Field.endField(builder);
  }

  /**
   * Gets the Arrow type from a given field.
   *
   * <p>It is necessary to get the same type as Arrow,
   * otherwise it is not quite possible to construct type offset as it is a
   * union of different types that have different structure
   *
   * @param field the field to get the Arrow type
   * @return      the Arrow type from a given field
   */
  private static org.apache.arrow.vector.types.pojo.ArrowType getTypeForField(org.apache.arrow.flatbuf.OldField field) {
    switch(field.typeType()) {
      case Type.Null: {
        org.apache.arrow.flatbuf.Null nullType = (org.apache.arrow.flatbuf.Null) field.type(new org.apache.arrow.flatbuf.Null());
        return new ArrowType.Null();
      }
      case Type.Struct_: {
        org.apache.arrow.flatbuf.Struct_ struct_Type = (org.apache.arrow.flatbuf.Struct_) field.type(new org.apache.arrow.flatbuf.Struct_());
        return new ArrowType.Struct();
      }
      case Type.List: {
        org.apache.arrow.flatbuf.List listType = (org.apache.arrow.flatbuf.List) field.type(new org.apache.arrow.flatbuf.List());
        return new ArrowType.List();
      }
      case Type.FixedSizeList: {
        org.apache.arrow.flatbuf.FixedSizeList fixedsizelistType = (org.apache.arrow.flatbuf.FixedSizeList) field.type(new org.apache.arrow.flatbuf.FixedSizeList());
        int listSize = fixedsizelistType.listSize();
        return new ArrowType.FixedSizeList(listSize);
      }
      case Type.Union: {
        org.apache.arrow.flatbuf.Union unionType = (org.apache.arrow.flatbuf.Union) field.type(new org.apache.arrow.flatbuf.Union());
        short mode = unionType.mode();
        int[] typeIds = new int[unionType.typeIdsLength()];
        for (int i = 0; i< typeIds.length; ++i) {
          typeIds[i] = unionType.typeIds(i);
        }
        return new ArrowType.Union(UnionMode.fromFlatbufID(mode), typeIds);
      }
      case Type.Int: {
        org.apache.arrow.flatbuf.Int intType = (org.apache.arrow.flatbuf.Int) field.type(new org.apache.arrow.flatbuf.Int());
        int bitWidth = intType.bitWidth();
        boolean isSigned = intType.isSigned();
        return new ArrowType.Int(bitWidth, isSigned);
      }
      case Type.FloatingPoint: {
        org.apache.arrow.flatbuf.FloatingPoint floatingpointType = (org.apache.arrow.flatbuf.FloatingPoint) field.type(new org.apache.arrow.flatbuf.FloatingPoint());
        short precision = floatingpointType.precision();
        return new ArrowType.FloatingPoint(FloatingPointPrecision.fromFlatbufID(precision));
      }
      case Type.Utf8: {
        org.apache.arrow.flatbuf.Utf8 utf8Type = (org.apache.arrow.flatbuf.Utf8) field.type(new org.apache.arrow.flatbuf.Utf8());
        return new ArrowType.Utf8();
      }
      case Type.Binary: {
        org.apache.arrow.flatbuf.Binary binaryType = (org.apache.arrow.flatbuf.Binary) field.type(new org.apache.arrow.flatbuf.Binary());
        return new ArrowType.Binary();
      }
      case Type.FixedSizeBinary: {
        org.apache.arrow.flatbuf.FixedSizeBinary fixedsizebinaryType = (org.apache.arrow.flatbuf.FixedSizeBinary) field.type(new org.apache.arrow.flatbuf.FixedSizeBinary());
        int byteWidth = fixedsizebinaryType.byteWidth();
        return new ArrowType.FixedSizeBinary(byteWidth);
      }
      case Type.Bool: {
        org.apache.arrow.flatbuf.Bool boolType = (org.apache.arrow.flatbuf.Bool) field.type(new org.apache.arrow.flatbuf.Bool());
        return new ArrowType.Bool();
      }
      case Type.Decimal: {
        org.apache.arrow.flatbuf.Decimal decimalType = (org.apache.arrow.flatbuf.Decimal) field.type(new org.apache.arrow.flatbuf.Decimal());
        int precision = decimalType.precision();
        int scale = decimalType.scale();
        return new ArrowType.Decimal(precision, scale);
      }
      case Type.Date: {
        org.apache.arrow.flatbuf.Date dateType = (org.apache.arrow.flatbuf.Date) field.type(new org.apache.arrow.flatbuf.Date());
        short unit = dateType.unit();
        return new ArrowType.Date(DateUnit.fromFlatbufID(unit));
      }
      case Type.Time: {
        org.apache.arrow.flatbuf.Time timeType = (org.apache.arrow.flatbuf.Time) field.type(new org.apache.arrow.flatbuf.Time());
        short unit = timeType.unit();
        int bitWidth = timeType.bitWidth();
        return new ArrowType.Time(TimeUnit.fromFlatbufID(unit), bitWidth);
      }
      case Type.Timestamp: {
        org.apache.arrow.flatbuf.Timestamp timestampType = (org.apache.arrow.flatbuf.Timestamp) field.type(new org.apache.arrow.flatbuf.Timestamp());
        short unit = timestampType.unit();
        String timezone = timestampType.timezone();
        return new ArrowType.Timestamp(TimeUnit.fromFlatbufID(unit), timezone);
      }
      case Type.Interval: {
        org.apache.arrow.flatbuf.Interval intervalType = (org.apache.arrow.flatbuf.Interval) field.type(new org.apache.arrow.flatbuf.Interval());
        short unit = intervalType.unit();
        return new ArrowType.Interval(IntervalUnit.fromFlatbufID(unit));
      }
      default:
        throw new UnsupportedOperationException("Unsupported type: " + field.typeType());
    }
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
