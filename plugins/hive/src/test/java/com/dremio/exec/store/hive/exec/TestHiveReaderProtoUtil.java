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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.RepeatTestRule;
import com.dremio.common.util.TestTools;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.PartitionXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.google.common.collect.Lists;

public class TestHiveReaderProtoUtil {

  @Rule
  public final TestRule REPEAT_RULE = TestTools.getRepeatRule(false);

  private static final int stringLength = 10;

  private static final String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  private static final Random random = new Random();

  private static String randomString(int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append(alphabet.charAt(random.nextInt(alphabet.length())));
    }
    return builder.toString();
  }

  private static Prop randomProp() {
    return Prop.newBuilder()
        .setKey(randomString(stringLength))
        .setValue(randomString(stringLength))
        .build();
  }

  private static List<Prop> randomListOfProps() {
    final int length = random.nextInt(stringLength);
    final List<Prop> props = Lists.newArrayList();
    for (int i = 0; i < length; i++) {
      props.add(randomProp());
    }
    return props;
  }

  private static PartitionXattr randomPartitionXattr() {
    final PartitionXattr.Builder builder = PartitionXattr.newBuilder();
    if (random.nextBoolean()) {
      builder.addAllPartitionProperty(randomListOfProps());
    }
    if (random.nextBoolean()) {
      builder.setInputFormat(randomString(stringLength));
    }
    if (random.nextBoolean()) {
      builder.setStorageHandler(randomString(stringLength));
    }
    if (random.nextBoolean()) {
      builder.setSerializationLib(randomString(stringLength));
    }
    return builder.build();
  }

  private static List<PartitionXattr> randomListOfPartitionXattrs() {
    final int length = random.nextInt(stringLength);
    final List<PartitionXattr> partitionXattrs = Lists.newArrayList();
    for (int i = 0; i < length; i++) {
      partitionXattrs.add(randomPartitionXattr());
    }
    return partitionXattrs;
  }

  @Test
  @RepeatTestRule.Repeat(count = 10)
  public void convert() {
    final HiveTableXattr.Builder builder = HiveTableXattr.newBuilder();
    if (random.nextBoolean()) {
      builder.addAllTableProperty(randomListOfProps());
    }
    if (random.nextBoolean()) {
      builder.setInputFormat(randomString(stringLength));
    }
    if (random.nextBoolean()) {
      builder.setStorageHandler(randomString(stringLength));
    }
    if (random.nextBoolean()) {
      builder.setSerializationLib(randomString(stringLength));
    }
    if (random.nextBoolean()) {
      builder.addAllPartitionXattrs(randomListOfPartitionXattrs());
    }

    final HiveTableXattr original = builder.build();

    HiveReaderProtoUtil.encodePropertiesAsDictionary(builder);
    final HiveTableXattr converted = builder.build();

    if (!original.getTablePropertyList().isEmpty()) {
      assertTrue(getMessage(original, converted),
          converted.getTablePropertyList().isEmpty());
      assertTrue(getMessage(original, converted),
          converted.getTablePropertySubscriptList().size() == original.getTablePropertyList().size());

      final List<Prop> convertedTableProperties = getTableProperties(converted);
      assertTrue(getMessage(original, converted),
          original.getTablePropertyList().size() == convertedTableProperties.size());

      for (int p = 0; p < original.getTablePropertyList().size(); p++) {
        assertTrue(getMessage(original, converted),
            convertedTableProperties.contains(original.getTableProperty((p))));
      }

      for (int p = 0; p < convertedTableProperties.size(); p++) {
        assertTrue(getMessage(original, converted),
            original.getTablePropertyList().contains(convertedTableProperties.get(p)));
      }
    }

    if (original.hasInputFormat()) {
      assertTrue(getMessage(original, converted), !converted.hasInputFormat());
      assertTrue(getMessage(original, converted), converted.hasTableInputFormatSubscript());
      assertTrue(getMessage(original, converted),
          HiveReaderProtoUtil.getTableInputFormat(converted).get()
              .equals(original.getInputFormat()));
    }

    if (original.hasSerializationLib()) {
      assertTrue(getMessage(original, converted), !converted.hasSerializationLib());
      assertTrue(getMessage(original, converted), converted.hasTableSerializationLibSubscript());
      assertTrue(getMessage(original, converted),
          HiveReaderProtoUtil.getTableSerializationLib(converted).get()
              .equals(original.getSerializationLib()));
    }

    if (original.hasStorageHandler()) {
      assertTrue(getMessage(original, converted), !converted.hasStorageHandler());
      assertTrue(getMessage(original, converted), converted.hasTableStorageHandlerSubscript());
      assertTrue(getMessage(original, converted),
          HiveReaderProtoUtil.getTableStorageHandler(converted).get()
              .equals(original.getStorageHandler()));
    }

    for (int i = 0; i < original.getPartitionXattrsList().size(); i++) {
      final PartitionXattr originalPartitionXattr = original.getPartitionXattrs(i);
      final PartitionXattr convertePartitionXattr = converted.getPartitionXattrs(i);

      if (!originalPartitionXattr.getPartitionPropertyList().isEmpty()) {
        assertTrue(getMessage(original, converted),
            convertePartitionXattr.getPartitionPropertyList().isEmpty());
        assertTrue(getMessage(original, converted),
            convertePartitionXattr.getPropertySubscriptList().size() == originalPartitionXattr.getPartitionPropertyList().size());

        final List<Prop> convertedPartitionProperties = getPartitionProperties(converted, i);
        assertTrue(getMessage(original, converted),
            originalPartitionXattr.getPartitionPropertyList().size() == convertedPartitionProperties.size());

        for (int p = 0; p < originalPartitionXattr.getPartitionPropertyList().size(); p++) {
          assertTrue(getMessage(original, converted),
              convertedPartitionProperties.contains(originalPartitionXattr.getPartitionProperty(p)));
        }

        for (int p = 0; p < convertedPartitionProperties.size(); p++) {
          assertTrue(getMessage(original, converted),
              originalPartitionXattr.getPartitionPropertyList().contains(convertedPartitionProperties.get(p)));
        }
      }

      if (originalPartitionXattr.hasInputFormat()) {
        assertTrue(getMessage(original, converted), !convertePartitionXattr.hasInputFormat());
        assertTrue(getMessage(original, converted), convertePartitionXattr.hasInputFormatSubscript());
        HiveReaderProtoUtil.getPartitionInputFormat(converted, i).get()
            .equals(originalPartitionXattr.getInputFormat());
      }

      if (originalPartitionXattr.hasStorageHandler()) {
        assertTrue(getMessage(original, converted), !convertePartitionXattr.hasStorageHandler());
        assertTrue(getMessage(original, converted), convertePartitionXattr.hasStorageHandlerSubscript());
        HiveReaderProtoUtil.getPartitionStorageHandler(converted, i).get()
            .equals(originalPartitionXattr.getStorageHandler());
      }

      if (originalPartitionXattr.hasSerializationLib()) {
        assertTrue(getMessage(original, converted), !convertePartitionXattr.hasSerializationLib());
        assertTrue(getMessage(original, converted), convertePartitionXattr.hasSerializationLibSubscript());
        HiveReaderProtoUtil.getPartitionSerializationLib(converted, i).get()
            .equals(originalPartitionXattr.getSerializationLib());
      }
    }
  }

  private static String getMessage(HiveTableXattr original, HiveTableXattr converted) {
    return "original: " + original.toString() + "\n"
        + "converted: " + converted.toString();
  }

  private static PartitionXattr newPartitionXattrWithInputFormat(String format) {
    return PartitionXattr.newBuilder()
        .setInputFormat(format)
        .build();
  }

  @Test
  public void buildInputFormatLookup() {

    {
      assertTrue(HiveReaderProtoUtil.buildInputFormatLookup(HiveTableXattr.newBuilder().build()).size() == 0);
    }

    {
      HiveTableXattr xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("b")))
          .build();
      Map<String, Integer> result = HiveReaderProtoUtil.buildInputFormatLookup(xattr);
      assertTrue(result.size() == 2);
      assertNotNull(result.get("a"));
      assertNotNull(result.get("b"));
    }

    {
      HiveTableXattr xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("b")))
          .setInputFormat("c")
          .build();
      Map<String, Integer> result = HiveReaderProtoUtil.buildInputFormatLookup(xattr);
      assertTrue(result.size() == 3);
      assertNotNull(result.get("a"));
      assertNotNull(result.get("b"));
      assertNotNull(result.get("c"));
    }

    {
      HiveTableXattr xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("b")))
          .setInputFormat("a")
          .build();
      Map<String, Integer> result = HiveReaderProtoUtil.buildInputFormatLookup(xattr);
      assertTrue(result.size() == 2);
      assertNotNull(result.get("a"));
      assertNotNull(result.get("b"));
    }
  }

  @Test
  public void getInputFormat() {
    {
      assertTrue(!HiveReaderProtoUtil.getTableInputFormat(HiveTableXattr.newBuilder().build()).isPresent());
      try {
        HiveReaderProtoUtil.getPartitionInputFormat(HiveTableXattr.newBuilder().build(), 0);
        fail();
      } catch (IllegalArgumentException ignored) {
      }

      assertTrue(!HiveReaderProtoUtil.getTableInputFormat(HiveTableXattr.newBuilder()
          .setPropertyCollectionType(HiveReaderProto.PropertyCollectionType.DICTIONARY)
          .build()).isPresent());
      try {
        HiveReaderProtoUtil.getPartitionInputFormat(HiveTableXattr.newBuilder()
            .setPropertyCollectionType(HiveReaderProto.PropertyCollectionType.DICTIONARY)
            .build(), 0);
        fail();
      } catch (IllegalArgumentException ignored) {
      }
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("b")));
      HiveTableXattr original = xattr.build();
      assertTrue(!HiveReaderProtoUtil.getTableInputFormat(original).isPresent());
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 1).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 2).get().equals("b"));

      HiveTableXattr converted = xattr.build();
      assertTrue(!HiveReaderProtoUtil.getTableInputFormat(converted).isPresent());
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 1).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 2).get().equals("b"));
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("b")))
          .setInputFormat("c");

      HiveTableXattr original = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableInputFormat(original).get().equals("c"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 1).get().equals("b"));

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableInputFormat(converted).get().equals("c"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 1).get().equals("b"));
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithInputFormat("a"),
                  newPartitionXattrWithInputFormat("b")))
          .setInputFormat("a");

      HiveTableXattr original = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableInputFormat(original).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 1).get().equals("b"));

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableInputFormat(converted).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 1).get().equals("b"));
    }
  }

  private static Prop newProp(String key, String value) {
    return Prop.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
  }

  private static PartitionXattr newPartitionXattrWithProps(Prop... props) {
    return PartitionXattr.newBuilder()
        .addAllPartitionProperty(Arrays.asList(props))
        .build();
  }

  @Test
  public void buildPropLookup() {
    {
      assertTrue(HiveReaderProtoUtil.buildPropLookup(HiveTableXattr.newBuilder().build()).size() == 0);
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithProps(newProp("a", "b")),
                  newPartitionXattrWithProps(newProp("c", "d"), newProp("a", "b"))));

      Map<Prop, Integer> lookup = HiveReaderProtoUtil.buildPropLookup(xattr);
      assertTrue(lookup.size() == 2);
      assertNotNull(lookup.get(newProp("a", "b")));
      assertNotNull(lookup.get(newProp("c", "d")));
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithProps(newProp("a", "b"), newProp("c", "d"))))
          .addAllTableProperty(
              Lists.newArrayList(
                  newProp("a", "c")));

      Map<Prop, Integer> lookup = HiveReaderProtoUtil.buildPropLookup(xattr);
      assertTrue(lookup.size() == 3);
      assertNotNull(lookup.get(newProp("a", "b")));
      assertNotNull(lookup.get(newProp("c", "d")));
      assertNotNull(lookup.get(newProp("a", "c")));
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithProps(newProp("a", "b"), newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d"), newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d"), newProp("c", "e")),
                  newPartitionXattrWithProps(newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d"))))
          .addAllTableProperty(
              Lists.newArrayList(
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "c")));

      Map<Prop, Integer> lookup = HiveReaderProtoUtil.buildPropLookup(xattr);
      assertTrue(lookup.size() == 4);
      assertNotNull(lookup.get(newProp("a", "b")));
      assertNotNull(lookup.get(newProp("c", "d")));
      assertNotNull(lookup.get(newProp("a", "c")));
      assertNotNull(lookup.get(newProp("c", "e")));
    }
  }

  @Test
  public void getProps() {
    {
      assertTrue(getTableProperties(HiveTableXattr.newBuilder().build()).size() == 0);
      try {
        HiveReaderProtoUtil.getPartitionProperties(HiveTableXattr.newBuilder().build(), 0);
        fail();
      } catch (IllegalArgumentException ignored) {
      }

      assertTrue(getTableProperties(HiveTableXattr.newBuilder()
          .setPropertyCollectionType(HiveReaderProto.PropertyCollectionType.DICTIONARY)
          .build()).size() == 0);
      try {
        HiveReaderProtoUtil.getPartitionProperties(HiveTableXattr.newBuilder()
            .setPropertyCollectionType(HiveReaderProto.PropertyCollectionType.DICTIONARY)
            .build(), 0);
        fail();
      } catch (IllegalArgumentException ignored) {
      }
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithProps(newProp("a", "b"), newProp("a", "b")),
                  newPartitionXattrWithProps(newProp("c", "d"), newProp("a", "b"))));

      HiveTableXattr original = xattr.build();
      assertTrue(getTableProperties(original).size() == 0);
      assertTrue(getPartitionProperties(original, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("a", "b"))));
      assertTrue(getPartitionProperties(original, 1)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("a", "b"))));
      try {
        HiveReaderProtoUtil.getPartitionProperties(original, -1);
        fail();
      } catch (IllegalArgumentException ignored) {
      }
      try {
        HiveReaderProtoUtil.getPartitionProperties(original, 2);
        fail();
      } catch (IllegalArgumentException ignored) {
      }

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(getTableProperties(converted).size() == 0);
      assertTrue(getPartitionProperties(converted, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("a", "b"))));
      assertTrue(getPartitionProperties(converted, 1)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("a", "b"))));
      try {
        HiveReaderProtoUtil.getPartitionProperties(original, -1);
        fail();
      } catch (IllegalArgumentException ignored) {
      }
      try {
        HiveReaderProtoUtil.getPartitionProperties(original, 2);
        fail();
      } catch (IllegalArgumentException ignored) {
      }
    }


    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithProps(newProp("a", "b"), newProp("c", "d"))))
          .addAllTableProperty(
              Lists.newArrayList(
                  newProp("a", "c")));

      HiveTableXattr original = xattr.build();
      assertTrue(getTableProperties(original)
          .equals(Lists.newArrayList(newProp("a", "c"))));
      assertTrue(getPartitionProperties(original, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(getTableProperties(converted)
          .equals(Lists.newArrayList(newProp("a", "c"))));
      assertTrue(getPartitionProperties(converted, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));
    }


    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
              Lists.newArrayList(
                  newPartitionXattrWithProps(newProp("a", "b"), newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d"), newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d"), newProp("c", "e")),
                  newPartitionXattrWithProps(newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d")),
                  newPartitionXattrWithProps(newProp("c", "d"))))
          .addAllTableProperty(
              Lists.newArrayList(
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "c")));

      HiveTableXattr original = xattr.build();
      assertTrue(getTableProperties(original)
          .equals(Lists.newArrayList(
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "c"))));

      assertTrue(getPartitionProperties(original, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));
      assertTrue(getPartitionProperties(original, 1)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "d"))));
      assertTrue(getPartitionProperties(original, 2)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "e"))));
      assertTrue(getPartitionProperties(original, 3)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(getPartitionProperties(original, 4)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(getPartitionProperties(original, 5)
          .equals(Lists.newArrayList(newProp("c", "d"))));

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(getTableProperties(converted)
          .equals(Lists.newArrayList(
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "c"))));

      assertTrue(getPartitionProperties(converted, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));
      assertTrue(getPartitionProperties(converted, 1)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "d"))));
      assertTrue(getPartitionProperties(converted, 2)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "e"))));
      assertTrue(getPartitionProperties(converted, 3)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(getPartitionProperties(converted, 4)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(getPartitionProperties(original, 5)
          .equals(Lists.newArrayList(newProp("c", "d"))));
    }
  }

  @Test
  public void isLegacyFormat() {
    {
      assertTrue(getTableProperties(HiveTableXattr.newBuilder().build()).size() == 0);
      try {
        HiveReaderProtoUtil.getPartitionProperties(HiveTableXattr.newBuilder().build(), 0);
        fail();
      } catch (IllegalArgumentException ignored) {
      }

      assertTrue(getTableProperties(HiveTableXattr.newBuilder()
        .setPropertyCollectionType(HiveReaderProto.PropertyCollectionType.DICTIONARY)
        .build()).size() == 0);
      try {
        HiveReaderProtoUtil.getPartitionProperties(HiveTableXattr.newBuilder()
          .setPropertyCollectionType(HiveReaderProto.PropertyCollectionType.DICTIONARY)
          .build(), 0);
        fail();
      } catch (IllegalArgumentException ignored) {
      }

      {
        HiveTableXattr tableXattr = HiveTableXattr.newBuilder()
          .addAllPartitionXattrs(
            Lists.newArrayList(
              newPartitionXattrWithProps(newProp("a", "b")),
              newPartitionXattrWithProps(newProp("c", "d"), newProp("a", "b")))).build();

        assertTrue(HiveReaderProtoUtil.isPreDremioVersion3dot2dot0LegacyFormat(tableXattr));
      }

      {
        HiveTableXattr tableXattr = HiveTableXattr.newBuilder()
          .addAllTableProperty(
            Lists.newArrayList(
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "c"))).build();

        assertFalse(HiveReaderProtoUtil.isPreDremioVersion3dot2dot0LegacyFormat(tableXattr));
      }
    }
  }

  private List<Prop> getPartitionProperties(HiveTableXattr attrs, int index) {
    return HiveReaderProtoUtil.getPartitionProperties(attrs, index).collect(Collectors.toList());
  }

  private List<Prop> getTableProperties(HiveTableXattr attrs) {
    return HiveReaderProtoUtil.getTableProperties(attrs).collect(Collectors.toList());
  }
}
