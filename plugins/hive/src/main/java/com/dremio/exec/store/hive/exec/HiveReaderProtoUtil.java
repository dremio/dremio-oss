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
package com.dremio.exec.store.hive.exec;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattrOrBuilder;
import com.dremio.hive.proto.HiveReaderProto.PartitionProp;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.PropertyCollectionType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Defines utility methods for classes in {@link com.dremio.hive.proto.HiveReaderProto}.
 */
public final class HiveReaderProtoUtil {

  /**
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
    final List<PartitionProp> newPartitionProps = Lists.newArrayList();
    for (final PartitionProp partitionProp : tableXattr.getPartitionPropertiesList()) {
      final PartitionProp.Builder builder = partitionProp.toBuilder();

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

      newPartitionProps.add(builder.build());
    }
    tableXattr.clearPartitionProperties(); // clear and then add!
    tableXattr.addAllPartitionProperties(newPartitionProps);

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

  static Map<Prop, Integer> buildPropLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<Prop, Integer> lookup = Maps.newHashMap();
    for (final PartitionProp partitionProp : tableXattr.getPartitionPropertiesList()) {
      for (final Prop prop : partitionProp.getPartitionPropertyList()) {
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

  static Map<String, Integer> buildInputFormatLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<String, Integer> lookup = Maps.newHashMap();
    for (final PartitionProp partitionProp : tableXattr.getPartitionPropertiesList()) {
      if (partitionProp.hasInputFormat() &&
          !lookup.containsKey(partitionProp.getInputFormat())) {
        lookup.put(partitionProp.getInputFormat(), i++);
      }
    }
    if (tableXattr.hasInputFormat()
        && !lookup.containsKey(tableXattr.getInputFormat())) {
      lookup.put(tableXattr.getInputFormat(), i);
    }

    return lookup;
  }

  static Map<String, Integer> buildStorageHandlerLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<String, Integer> lookup = Maps.newHashMap();
    for (final PartitionProp partitionProp : tableXattr.getPartitionPropertiesList()) {
      if (partitionProp.hasStorageHandler() &&
          !lookup.containsKey(partitionProp.getStorageHandler())) {
        lookup.put(partitionProp.getStorageHandler(), i++);
      }
    }
    if (tableXattr.hasStorageHandler()
        && !lookup.containsKey(tableXattr.getStorageHandler())) {
      lookup.put(tableXattr.getStorageHandler(), i);
    }

    return lookup;
  }

  static Map<String, Integer> buildSerializationLibLookup(HiveTableXattrOrBuilder tableXattr) {
    int i = 0;
    final Map<String, Integer> lookup = Maps.newHashMap();
    for (final PartitionProp partitionProp : tableXattr.getPartitionPropertiesList()) {
      if (partitionProp.hasSerializationLib() &&
          !lookup.containsKey(partitionProp.getSerializationLib())) {
        lookup.put(partitionProp.getSerializationLib(), i++);
      }
    }
    if (tableXattr.hasSerializationLib()
        && !lookup.containsKey(tableXattr.getSerializationLib())) {
      lookup.put(tableXattr.getSerializationLib(), i);
    }

    return lookup;
  }

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
   * @param tableXattr  hive table extended attribute
   * @param partitionIndex partition index
   * @return list of properties
   */
  public static List<Prop> getPartitionProperties(final HiveTableXattr tableXattr, int partitionIndex) {
    Preconditions.checkArgument(partitionIndex >= 0 && partitionIndex < tableXattr.getPartitionPropertiesList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionProperties(partitionIndex)
          .getPartitionPropertyList();
    }

    return FluentIterable.from(tableXattr.getPartitionProperties(partitionIndex)
        .getPropertySubscriptList())
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
   * Get the input format for the given partition id.
   *
   * @param tableXattr  hive table extended attribute
   * @param partitionId partition id
   * @return input format
   */
  public static Optional<String> getPartitionInputFormat(final HiveTableXattr tableXattr, int partitionId) {
    Preconditions.checkArgument(partitionId >= 0 && partitionId < tableXattr.getPartitionPropertiesList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionProperties(partitionId).hasInputFormat()
          ? Optional.of(tableXattr.getPartitionProperties(partitionId).getInputFormat())
          : Optional.<String>absent();
    }

    return tableXattr.getPartitionProperties(partitionId).hasInputFormatSubscript()
        ? Optional.of(tableXattr.getInputFormatDictionary(
        tableXattr.getPartitionProperties(partitionId)
            .getInputFormatSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the storage handler for the given partition id.
   *
   * @param tableXattr  hive table extended attribute
   * @param partitionId partition id
   * @return storage handler
   */
  public static Optional<String> getPartitionStorageHandler(final HiveTableXattr tableXattr, int partitionId) {
    Preconditions.checkArgument(partitionId >= 0 && partitionId < tableXattr.getPartitionPropertiesList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionProperties(partitionId).hasStorageHandler()
          ? Optional.of(tableXattr.getPartitionProperties(partitionId).getStorageHandler())
          : Optional.<String>absent();
    }

    return tableXattr.getPartitionProperties(partitionId).hasStorageHandlerSubscript()
        ? Optional.of(tableXattr.getStorageHandlerDictionary(
        tableXattr.getPartitionProperties(partitionId)
            .getStorageHandlerSubscript()))
        : Optional.<String>absent();
  }

  /**
   * Get the serialization lib for the given partition id.
   *
   * @param tableXattr  hive table extended attribute
   * @param partitionId partition id
   * @return serialization lib
   */
  public static Optional<String> getPartitionSerializationLib(final HiveTableXattr tableXattr, int partitionId) {
    Preconditions.checkArgument(partitionId >= 0 && partitionId < tableXattr.getPartitionPropertiesList().size());

    if (tableXattr.getPropertyCollectionType() == PropertyCollectionType.LIST) { // backward compatibility
      return tableXattr.getPartitionProperties(partitionId).hasSerializationLib()
          ? Optional.of(tableXattr.getPartitionProperties(partitionId).getSerializationLib())
          : Optional.<String>absent();
    }

    return tableXattr.getPartitionProperties(partitionId).hasSerializationLibSubscript()
        ? Optional.of(tableXattr.getSerializationLibDictionary(
        tableXattr.getPartitionProperties(partitionId)
            .getSerializationLibSubscript()))
        : Optional.<String>absent();
  }

  private HiveReaderProtoUtil() {
  }
}
