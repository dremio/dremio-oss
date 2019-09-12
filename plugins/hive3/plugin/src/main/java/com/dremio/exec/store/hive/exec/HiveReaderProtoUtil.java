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
package com.dremio.exec.store.hive.exec;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattrOrBuilder;
import com.dremio.hive.proto.HiveReaderProto.PartitionXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.PropertyCollectionType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Defines utility methods for classes in {@link com.dremio.hive.proto.HiveReaderProto}.
 */
public final class HiveReaderProtoUtil {

  /**
   * This method is marked as deprecated as the partitionChunks now have all partition properties and other
   * previously-dictionary-enabled properties as explicit lists or values.
   * Converts properties in the given {@link HiveTableXattr hive table attribute} from list-style to dictionary-style.
   * The attribute contains fields that correspond to properties of a hive table. These properties are maintained
   * as lists at table-level and partition-level, and most of the values are equal across partitions. This method
   * creates a dictionary of groups of values at top-level, and then, replaces actual values with subscripts of values
   * that correspond to indexes in the respective dictionary. Use other utility methods (like
   * {@link #getTableInputFormat}, etc) defined in this class to access the values; API consumers of those methods
   * need not be aware of internal list/ dictionary style.
   *
   * @param tableXattr hive table extended attribute
   */
  @Deprecated
  public static void encodePropertiesAsDictionary(HiveTableXattr.Builder tableXattr) {
    // (1) create dictionaries and lookup maps of properties
    final Map<Prop, Integer> propLookup = buildPropLookup(tableXattr);
    tableXattr.addAllPropertyDictionary(lookupToList(Prop.class, propLookup));

    final Map<String, Integer> formatLookup = buildInputFormatLookup(tableXattr);
    tableXattr.addAllInputFormatDictionary(lookupToList(String.class, formatLookup));

    final Map<String, Integer> handlerLookup = buildStorageHandlerLookup(tableXattr);
    tableXattr.addAllStorageHandlerDictionary(lookupToList(String.class, handlerLookup));

    final Map<String, Integer> libLookup = buildSerializationLibLookup(tableXattr);
    tableXattr.addAllSerializationLibDictionary(lookupToList(String.class, libLookup));

    // (2) rewrite partitions to use the above dictionaries
    final List<PartitionXattr> partitionXattrs = Lists.newArrayList();
    for (final PartitionXattr partitionXattr : tableXattr.getPartitionXattrsList()) {
      final PartitionXattr.Builder builder = partitionXattr.toBuilder();

      final List<Integer> propertySubscripts = Lists.newArrayList();
      for (final Prop prop : builder.getPartitionPropertyList()) {
        propertySubscripts.add(propLookup.get(prop));
      }
      builder.addAllPropertySubscript(propertySubscripts);
      builder.clearPartitionProperty();

      if (builder.hasInputFormat()) {
        builder.setInputFormatSubscript(formatLookup.get(builder.getInputFormat()));
        builder.clearInputFormat();
      }

      if (builder.hasStorageHandler()) {
        builder.setStorageHandlerSubscript(handlerLookup.get(builder.getStorageHandler()));
        builder.clearStorageHandler();
      }

      if (builder.hasSerializationLib()) {
        builder.setSerializationLibSubscript(libLookup.get(builder.getSerializationLib()));
        builder.clearSerializationLib();
      }

      partitionXattrs.add(builder.build());
    }
    tableXattr.clearPartitionXattrs(); // clear and then add!
    tableXattr.addAllPartitionXattrs(partitionXattrs);

    // (3) rewrite table level properties to use the above dictionaries
    final List<Integer> tableSubscripts = Lists.newArrayList();
    for (final Prop tableProp : tableXattr.getTablePropertyList()) {
      tableSubscripts.add(propLookup.get(tableProp));
    }
    tableXattr.addAllTablePropertySubscript(tableSubscripts);
    tableXattr.clearTableProperty();

    if (tableXattr.hasInputFormat()) {
      tableXattr.setTableInputFormatSubscript(formatLookup.get(tableXattr.getInputFormat()));
      tableXattr.clearInputFormat();
    }

    if (tableXattr.hasStorageHandler()) {
      tableXattr.setTableStorageHandlerSubscript(handlerLookup.get(tableXattr.getStorageHandler()));
      tableXattr.clearStorageHandler();
    }

    if (tableXattr.hasSerializationLib()) {
      tableXattr.setTableSerializationLibSubscript(libLookup.get(tableXattr.getSerializationLib()));
      tableXattr.clearSerializationLib();
    }

    // (4) mark as dictionary based lookup
    tableXattr.setPropertyCollectionType(PropertyCollectionType.DICTIONARY);
  }

  /**
   * This method is marked as deprecated as the partitionChunks now have all partition properties and other
   * previously-dictionary-enabled properties as explicit lists or values.
   */
  @Deprecated
  static Map<Prop, Integer> buildPropLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<Prop, Integer> lookup = Maps.newHashMap();
    for (final PartitionXattr partitionXattr : tableXattr.getPartitionXattrsList()) {
      for (final Prop prop : partitionXattr.getPartitionPropertyList()) {
        if (!lookup.containsKey(prop)) {
          lookup.put(prop, i++);
        }
      }
    }
    for (final Prop tableProp : tableXattr.getTablePropertyList()) {
      if (!lookup.containsKey(tableProp)) {
        lookup.put(tableProp, i++);
      }
    }

    return lookup;
  }

  /**
   * This method is marked as deprecated as the partitionChunks now have all partition properties and other
   * previously-dictionary-enabled properties as explicit lists or values.
   */
  @Deprecated
  static Map<String, Integer> buildInputFormatLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<String, Integer> lookup = Maps.newHashMap();
    for (final PartitionXattr partitionXattr : tableXattr.getPartitionXattrsList()) {
      if (partitionXattr.hasInputFormat() &&
        !lookup.containsKey(partitionXattr.getInputFormat())) {
        lookup.put(partitionXattr.getInputFormat(), i++);
      }
    }
    if (tableXattr.hasInputFormat()
        && !lookup.containsKey(tableXattr.getInputFormat())) {
      lookup.put(tableXattr.getInputFormat(), i);
    }

    return lookup;
  }

  /**
   * This method is marked as deprecated as the partitionChunks now have all partition properties and other
   * previously-dictionary-enabled properties as explicit lists or values.
   */
  @Deprecated
  static Map<String, Integer> buildStorageHandlerLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<String, Integer> lookup = Maps.newHashMap();
    for (final PartitionXattr partitionXattr : tableXattr.getPartitionXattrsList()) {
      if (partitionXattr.hasStorageHandler() &&
        !lookup.containsKey(partitionXattr.getStorageHandler())) {
        lookup.put(partitionXattr.getStorageHandler(), i++);
      }
    }
    if (tableXattr.hasStorageHandler()
        && !lookup.containsKey(tableXattr.getStorageHandler())) {
      lookup.put(tableXattr.getStorageHandler(), i);
    }

    return lookup;
  }

  /**
   * This method is marked as deprecated as the partitionChunks now have all partition properties and other
   * previously-dictionary-enabled properties as explicit lists or values.
   */
  @Deprecated
  static Map<String, Integer> buildSerializationLibLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<String, Integer> lookup = Maps.newHashMap();
    for (final PartitionXattr partitionXattr : tableXattr.getPartitionXattrsList()) {
      if (partitionXattr.hasSerializationLib() &&
        !lookup.containsKey(partitionXattr.getSerializationLib())) {
        lookup.put(partitionXattr.getSerializationLib(), i++);
      }
    }
    if (tableXattr.hasSerializationLib()
        && !lookup.containsKey(tableXattr.getSerializationLib())) {
      lookup.put(tableXattr.getSerializationLib(), i);
    }

    return lookup;
  }

  /**
   * This method is marked as deprecated as the partitionChunks now have all partition properties and other
   * previously-dictionary-enabled properties as explicit lists or values.
   */
  @Deprecated
  private static <T> List<T> lookupToList(Class<T> clazz, Map<T, Integer> lookup) {
    @SuppressWarnings("unchecked") final T[] array = (T[]) Array.newInstance(clazz, lookup.size());
    for (final Map.Entry<T, Integer> entry : lookup.entrySet()) {
      array[entry.getValue()] = entry.getKey();
    }
    return Arrays.asList(array);
  }

  /**
   * Get the list of table properties from the given attribute.
   *
   * @param tableXattr hive table extended attribute
   * @return list of table properties
   */
  public static List<Prop> getTableProperties(final HiveTableXattr tableXattr) {
    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getTablePropertyList();
    }

    return FluentIterable.from(tableXattr.getTablePropertySubscriptList())
        .transform(new Function<Integer, Prop>() {
          @Override
          public Prop apply(Integer input) {
            return Prop.newBuilder()
                .setKey(tableXattr.getPropertyDictionary(input)
                    .getKey())
                .setValue(tableXattr.getPropertyDictionary(input)
                    .getValue())
                .build();
          }
        }).toList();
  }

  /**
   * Get the table input format.
   *
   * @param tableXattr hive table extended attribute
   * @return table input format
   */
  public static Optional<String> getTableInputFormat(final HiveTableXattr tableXattr) {
    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.hasInputFormat() ? Optional.of(tableXattr.getInputFormat()) : Optional.<String>absent();
    }

    return tableXattr.hasTableInputFormatSubscript()
        ? Optional.of(tableXattr.getInputFormatDictionary(tableXattr.getTableInputFormatSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the table storage handler.
   *
   * @param tableXattr hive table extended attribute
   * @return table storage handler
   */
  public static Optional<String> getTableStorageHandler(final HiveTableXattr tableXattr) {
    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.hasStorageHandler() ? Optional.of(tableXattr.getStorageHandler()) : Optional.<String>absent();
    }

    return tableXattr.hasTableStorageHandlerSubscript()
        ? Optional.of(tableXattr.getStorageHandlerDictionary(tableXattr.getTableStorageHandlerSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the table serialization library.
   *
   * @param tableXattr hive table extended attribute
   * @return table serialization library
   */
  public static Optional<String> getTableSerializationLib(final HiveTableXattr tableXattr) {
    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.hasSerializationLib()
          ? Optional.of(tableXattr.getSerializationLib())
          : Optional.<String>absent();
    }

    return tableXattr.hasTableSerializationLibSubscript()
        ? Optional.of(tableXattr.getSerializationLibDictionary(tableXattr.getTableSerializationLibSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the list of properties for the given partition index.
   *
   * @param tableXattr hive table extended attribute
   * @param partitionIndex partition index
   * @return list of properties
   */
  public static List<Prop> getPartitionProperties(final HiveTableXattr tableXattr, int partitionIndex) {
    Preconditions.checkArgument(partitionIndex >= 0 && partitionIndex < tableXattr.getPartitionXattrsList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionXattrs(partitionIndex)
        .getPartitionPropertyList();
    }

    return getPartitionProperties(tableXattr, tableXattr.getPartitionXattrs(partitionIndex));
  }


  /**
   * Get the list of properties for the given partition.
   *
   * @param tableXattr hive table extended attribute
   * @param partitionXattr hive partition extended attribute
   * @return list of properties
   */
  public static List<Prop> getPartitionProperties(final HiveTableXattr tableXattr, final PartitionXattr partitionXattr) {
    return partitionXattr.getPropertySubscriptList().stream()
      .map(input -> Prop.newBuilder()
        .setKey(tableXattr.getPropertyDictionary(input).getKey())
        .setValue(tableXattr.getPropertyDictionary(input).getValue())
        .build())
      .collect(Collectors.toList());
  }

  /**
   * Get the input format for the given partition id.
   *
   * @param tableXattr  hive table extended attribute
   * @param partitionId partition id
   * @return input format
   */
  public static Optional<String> getPartitionInputFormat(final HiveTableXattr tableXattr, int partitionId) {
    Preconditions.checkArgument(partitionId >= 0 && partitionId < tableXattr.getPartitionXattrsList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionXattrs(partitionId).hasInputFormat()
          ? Optional.of(tableXattr.getPartitionXattrs(partitionId).getInputFormat())
          : Optional.<String>absent();
    }

    return tableXattr.getPartitionXattrs(partitionId).hasInputFormatSubscript()
        ? Optional.of(tableXattr.getInputFormatDictionary(
        tableXattr.getPartitionXattrs(partitionId)
            .getInputFormatSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the input format for the given partition.
   *
   * @param tableXattr hive table extended attribute
   * @param partitionXattr hive partition extended attribute
   * @return input format
   */
  public static Optional<String> getPartitionInputFormat(final HiveTableXattr tableXattr, final PartitionXattr partitionXattr) {
    if(partitionXattr.hasInputFormatSubscript()) {
      return Optional.of(tableXattr.getInputFormatDictionary(partitionXattr.getInputFormatSubscript()));
    } else {
      return Optional.of(partitionXattr.getInputFormat());
    }
  }

  /**
   * Get the storage handler for the given partition id.
   *
   * @param tableXattr  hive table extended attribute
   * @param partitionId partition id
   * @return storage handler
   */
  public static Optional<String> getPartitionStorageHandler(final HiveTableXattr tableXattr, int partitionId) {
    Preconditions.checkArgument(partitionId >= 0 && partitionId < tableXattr.getPartitionXattrsList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionXattrs(partitionId).hasStorageHandler()
          ? Optional.of(tableXattr.getPartitionXattrs(partitionId).getStorageHandler())
          : Optional.<String>absent();
    }

    return tableXattr.getPartitionXattrs(partitionId).hasStorageHandlerSubscript()
        ? Optional.of(tableXattr.getStorageHandlerDictionary(
        tableXattr.getPartitionXattrs(partitionId)
            .getStorageHandlerSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the storage handler for the given partition.
   *
   * @param tableXattr hive table extended attribute
   * @param partitionXattr hive partition extended attribute
   * @return storage handler
   */
  public static Optional<String> getPartitionStorageHandler(final HiveTableXattr tableXattr, final PartitionXattr partitionXattr) {
    if(partitionXattr.hasStorageHandlerSubscript()) {
      return Optional.of(tableXattr.getStorageHandlerDictionary(partitionXattr.getStorageHandlerSubscript()));
    } else {
      return Optional.of(partitionXattr.getStorageHandler());
    }
  }

  /**
   * Get the serialization lib for the given partition id.
   *
   * @param tableXattr  hive table extended attribute
   * @param partitionId partition id
   * @return serialization lib
   */
  public static Optional<String> getPartitionSerializationLib(final HiveTableXattr tableXattr, int partitionId) {
    Preconditions.checkArgument(partitionId >= 0 && partitionId < tableXattr.getPartitionXattrsList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionXattrs(partitionId).hasSerializationLib()
          ? Optional.of(tableXattr.getPartitionXattrs(partitionId).getSerializationLib())
          : Optional.<String>absent();
    }

    return tableXattr.getPartitionXattrs(partitionId).hasSerializationLibSubscript()
        ? Optional.of(tableXattr.getSerializationLibDictionary(
        tableXattr.getPartitionXattrs(partitionId)
            .getSerializationLibSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the serialization lib for the given partition.
   *
   * @param tableXattr hive table extended attribute
   * @param partitionXattr hive partition extended attribute
   * @return serialization lib
   */
  public static String getPartitionSerializationLib(final HiveTableXattr tableXattr, final PartitionXattr partitionXattr) {
    if(partitionXattr.hasSerializationLibSubscript()) {
      return tableXattr.getSerializationLibDictionary(partitionXattr.getSerializationLibSubscript());
    } else {
      return partitionXattr.getSerializationLib();
    }
  }

  /**
   * Get the partition extended attribute for the given SplitInfo.
   *
   * @param splitInfo Dataset splitInfo
   * @return partition extended attribute
   */
  public static PartitionXattr getPartitionXattr(SplitAndPartitionInfo split) {
    final PartitionXattr partitionXattr;
    try {
      partitionXattr = PartitionXattr.parseFrom(split.getPartitionInfo().getExtendedProperty());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(new ExecutionSetupException("Failure parsing partition extended properties.", e));
    }
    return partitionXattr;
  }


  /**
   * Check if the partition properties are stored on DatasetMetadata(Pre 3.2.0)
   * or PartitionChunk(version 3.2.0 and above). When the partition properties
   * are stored on PartitionChunk tableXattr.getPartitionXattrsList() is always
   * empty.
   *
   * @param tableXattr hive table extended attribute
   * @return true if the partition properties are stored on DatasetMetadata
   */
  public static boolean isPreDremioVersion3dot2dot0LegacyFormat(final HiveTableXattr tableXattr) {
    return !tableXattr.getPartitionXattrsList().isEmpty();
  }

  private HiveReaderProtoUtil() {
  }
}
