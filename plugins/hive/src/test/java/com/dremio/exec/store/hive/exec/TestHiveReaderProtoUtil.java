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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.RepeatTestRule;
import com.dremio.common.util.TestTools;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.PartitionProp;
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

  private static PartitionProp randomPartitionProp() {
    final PartitionProp.Builder builder = PartitionProp.newBuilder();
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

  private static List<PartitionProp> randomListOfPartitionProps() {
    final int length = random.nextInt(stringLength);
    final List<PartitionProp> props = Lists.newArrayList();
    for (int i = 0; i < length; i++) {
      props.add(randomPartitionProp());
    }
    return props;
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
      builder.addAllPartitionProperties(randomListOfPartitionProps());
    }

    final HiveTableXattr original = builder.build();

    HiveReaderProtoUtil.encodePropertiesAsDictionary(builder);
    final HiveTableXattr converted = builder.build();

    if (!original.getTablePropertyList().isEmpty()) {
      assertTrue(getMessage(original, converted),
          converted.getTablePropertyList().isEmpty());
      assertTrue(getMessage(original, converted),
          converted.getTablePropertySubscriptList().size() == original.getTablePropertyList().size());

      final List<Prop> convertedTableProperties = HiveReaderProtoUtil.getTableProperties(converted);
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

    for (int i = 0; i < original.getPartitionPropertiesList().size(); i++) {
      final PartitionProp originalProp = original.getPartitionProperties(i);
      final PartitionProp convertedProp = converted.getPartitionProperties(i);

      if (!originalProp.getPartitionPropertyList().isEmpty()) {
        assertTrue(getMessage(original, converted),
            convertedProp.getPartitionPropertyList().isEmpty());
        assertTrue(getMessage(original, converted),
            convertedProp.getPropertySubscriptList().size() == originalProp.getPartitionPropertyList().size());

        final List<Prop> convertedPartitionProperties = HiveReaderProtoUtil.getPartitionProperties(converted, i);
        assertTrue(getMessage(original, converted),
            originalProp.getPartitionPropertyList().size() == convertedPartitionProperties.size());

        for (int p = 0; p < originalProp.getPartitionPropertyList().size(); p++) {
          assertTrue(getMessage(original, converted),
              convertedPartitionProperties.contains(originalProp.getPartitionProperty(p)));
        }

        for (int p = 0; p < convertedPartitionProperties.size(); p++) {
          assertTrue(getMessage(original, converted),
              originalProp.getPartitionPropertyList().contains(convertedPartitionProperties.get(p)));
        }
      }

      if (originalProp.hasInputFormat()) {
        assertTrue(getMessage(original, converted), !convertedProp.hasInputFormat());
        assertTrue(getMessage(original, converted), convertedProp.hasInputFormatSubscript());
        HiveReaderProtoUtil.getPartitionInputFormat(converted, i).get()
            .equals(originalProp.getInputFormat());
      }

      if (originalProp.hasStorageHandler()) {
        assertTrue(getMessage(original, converted), !convertedProp.hasStorageHandler());
        assertTrue(getMessage(original, converted), convertedProp.hasStorageHandlerSubscript());
        HiveReaderProtoUtil.getPartitionStorageHandler(converted, i).get()
            .equals(originalProp.getStorageHandler());
      }

      if (originalProp.hasSerializationLib()) {
        assertTrue(getMessage(original, converted), !convertedProp.hasSerializationLib());
        assertTrue(getMessage(original, converted), convertedProp.hasSerializationLibSubscript());
        HiveReaderProtoUtil.getPartitionSerializationLib(converted, i).get()
            .equals(originalProp.getSerializationLib());
      }
    }
  }

  private static String getMessage(HiveTableXattr original, HiveTableXattr converted) {
    return "original: " + original.toString() + "\n"
        + "converted: " + converted.toString();
  }

  private static PartitionProp newPartitionPropWithInputFormat(String format) {
    return PartitionProp.newBuilder()
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("b")))
          .build();
      Map<String, Integer> result = HiveReaderProtoUtil.buildInputFormatLookup(xattr);
      assertTrue(result.size() == 2);
      assertNotNull(result.get("a"));
      assertNotNull(result.get("b"));
    }

    {
      HiveTableXattr xattr = HiveTableXattr.newBuilder()
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("b")))
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("b")))
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("b")));
      HiveTableXattr original = xattr.build();
      assertTrue(!HiveReaderProtoUtil.getTableInputFormat(original).isPresent());
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 1).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(original, 2).get().equals("b"));

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(!HiveReaderProtoUtil.getTableInputFormat(converted).isPresent());
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 0).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 1).get().equals("a"));
      assertTrue(HiveReaderProtoUtil.getPartitionInputFormat(converted, 2).get().equals("b"));
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("b")))
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithInputFormat("a"),
                  newPartitionPropWithInputFormat("b")))
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

  private static PartitionProp newPartitionPropWithProps(Prop... props) {
    return PartitionProp.newBuilder()
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithProps(newProp("a", "b")),
                  newPartitionPropWithProps(newProp("c", "d"), newProp("a", "b"))));

      Map<Prop, Integer> lookup = HiveReaderProtoUtil.buildPropLookup(xattr);
      assertTrue(lookup.size() == 2);
      assertNotNull(lookup.get(newProp("a", "b")));
      assertNotNull(lookup.get(newProp("c", "d")));
    }

    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithProps(newProp("a", "b"), newProp("c", "d"))))
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithProps(newProp("a", "b"), newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d"), newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d"), newProp("c", "e")),
                  newPartitionPropWithProps(newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d"))))
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
      assertTrue(HiveReaderProtoUtil.getTableProperties(HiveTableXattr.newBuilder().build()).size() == 0);
      try {
        HiveReaderProtoUtil.getPartitionProperties(HiveTableXattr.newBuilder().build(), 0);
        fail();
      } catch (IllegalArgumentException ignored) {
      }

      assertTrue(HiveReaderProtoUtil.getTableProperties(HiveTableXattr.newBuilder()
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithProps(newProp("a", "b"), newProp("a", "b")),
                  newPartitionPropWithProps(newProp("c", "d"), newProp("a", "b"))));

      HiveTableXattr original = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableProperties(original).size() == 0);
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("a", "b"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 1)
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
      assertTrue(HiveReaderProtoUtil.getTableProperties(converted).size() == 0);
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("a", "b"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 1)
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
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithProps(newProp("a", "b"), newProp("c", "d"))))
          .addAllTableProperty(
              Lists.newArrayList(
                  newProp("a", "c")));

      HiveTableXattr original = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableProperties(original)
          .equals(Lists.newArrayList(newProp("a", "c"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableProperties(converted)
          .equals(Lists.newArrayList(newProp("a", "c"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));
    }


    {
      HiveTableXattr.Builder xattr = HiveTableXattr.newBuilder()
          .addAllPartitionProperties(
              Lists.newArrayList(
                  newPartitionPropWithProps(newProp("a", "b"), newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d"), newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d"), newProp("c", "e")),
                  newPartitionPropWithProps(newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d")),
                  newPartitionPropWithProps(newProp("c", "d"))))
          .addAllTableProperty(
              Lists.newArrayList(
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "b"),
                  newProp("a", "c")));

      HiveTableXattr original = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableProperties(original)
          .equals(Lists.newArrayList(
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "c"))));

      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 1)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 2)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "e"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 3)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 4)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 5)
          .equals(Lists.newArrayList(newProp("c", "d"))));

      HiveReaderProtoUtil.encodePropertiesAsDictionary(xattr);
      HiveTableXattr converted = xattr.build();
      assertTrue(HiveReaderProtoUtil.getTableProperties(converted)
          .equals(Lists.newArrayList(
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "b"),
              newProp("a", "c"))));

      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 0)
          .equals(Lists.newArrayList(newProp("a", "b"), newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 1)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 2)
          .equals(Lists.newArrayList(newProp("c", "d"), newProp("c", "e"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 3)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(converted, 4)
          .equals(Lists.newArrayList(newProp("c", "d"))));
      assertTrue(HiveReaderProtoUtil.getPartitionProperties(original, 5)
          .equals(Lists.newArrayList(newProp("c", "d"))));
    }
  }
}
