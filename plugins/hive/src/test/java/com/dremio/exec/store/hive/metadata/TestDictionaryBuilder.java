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
package com.dremio.exec.store.hive.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;

import org.junit.Test;

import com.dremio.hive.proto.HiveReaderProto;

public class TestDictionaryBuilder {

  @Test
  public void testEmpty() {
    DictionaryBuilder<String> builder = new DictionaryBuilder<>(String.class);
    Iterator<String> iterator = builder.build().iterator();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testStringValues_3Distinct() {
    DictionaryBuilder<String> builder = new DictionaryBuilder<>(String.class);
    assertEquals(0, builder.getOrCreateSubscript("1"));
    assertEquals(1, builder.getOrCreateSubscript("2"));
    assertEquals(2, builder.getOrCreateSubscript("3"));

    Iterator<String> iterator = builder.build().iterator();
    assertEquals("1", iterator.next());
    assertEquals("2", iterator.next());
    assertEquals("3", iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testStringValues_3DistinctReverse() {
    DictionaryBuilder<String> builder = new DictionaryBuilder<>(String.class);
    assertEquals(0, builder.getOrCreateSubscript("3"));
    assertEquals(1, builder.getOrCreateSubscript("2"));
    assertEquals(2, builder.getOrCreateSubscript("1"));

    Iterator<String> iterator = builder.build().iterator();
    assertEquals("3", iterator.next());
    assertEquals("2", iterator.next());
    assertEquals("1", iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testStringValues_3Identical() {
    DictionaryBuilder<String> builder = new DictionaryBuilder<>(String.class);
    assertEquals(0, builder.getOrCreateSubscript("1"));
    assertEquals(0, builder.getOrCreateSubscript("1"));
    assertEquals(0, builder.getOrCreateSubscript("1"));

    Iterator<String> iterator = builder.build().iterator();
    assertEquals("1", iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testStringValues_3Repeating() {
    DictionaryBuilder<String> builder = new DictionaryBuilder<>(String.class);
    assertEquals(0, builder.getOrCreateSubscript("1"));
    assertEquals(1, builder.getOrCreateSubscript("2"));
    assertEquals(2, builder.getOrCreateSubscript("3"));
    assertEquals(0, builder.getOrCreateSubscript("1"));
    assertEquals(0, builder.getOrCreateSubscript("1"));
    assertEquals(1, builder.getOrCreateSubscript("2"));
    assertEquals(2, builder.getOrCreateSubscript("3"));
    assertEquals(2, builder.getOrCreateSubscript("3"));
    assertEquals(2, builder.getOrCreateSubscript("3"));
    assertEquals(0, builder.getOrCreateSubscript("1"));
    assertEquals(1, builder.getOrCreateSubscript("2"));
    assertEquals(1, builder.getOrCreateSubscript("2"));
    assertEquals(2, builder.getOrCreateSubscript("3"));

    Iterator<String> iterator = builder.build().iterator();
    assertEquals("1", iterator.next());
    assertEquals("2", iterator.next());
    assertEquals("3", iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testPartitionPropValues_3Distinct() {
    DictionaryBuilder<HiveReaderProto.Prop> builder = new DictionaryBuilder<>(HiveReaderProto.Prop.class);
    assertEquals(0, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build()));
    assertEquals(1, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build()));
    assertEquals(2, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build()));

    Iterator<HiveReaderProto.Prop> iterator = builder.build().iterator();
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build(), iterator.next());
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build(), iterator.next());
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build(), iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testPartitionPropValues_3DistinctReverse() {
    DictionaryBuilder<HiveReaderProto.Prop> builder = new DictionaryBuilder<>(HiveReaderProto.Prop.class);
    assertEquals(0, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build()));
    assertEquals(1, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build()));
    assertEquals(2, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build()));

    Iterator<HiveReaderProto.Prop> iterator = builder.build().iterator();
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build(), iterator.next());
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build(), iterator.next());
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build(), iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testPartitionPropValues_3Repeating() {
    DictionaryBuilder<HiveReaderProto.Prop> builder = new DictionaryBuilder<>(HiveReaderProto.Prop.class);
    assertEquals(0, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build()));
    assertEquals(1, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build()));
    assertEquals(2, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build()));
    assertEquals(2, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build()));
    assertEquals(1, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build()));
    assertEquals(2, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build()));
    assertEquals(1, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build()));
    assertEquals(1, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build()));
    assertEquals(0, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build()));
    assertEquals(0, builder.getOrCreateSubscript(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build()));

    Iterator<HiveReaderProto.Prop> iterator = builder.build().iterator();
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key3").setValue("value3").build(), iterator.next());
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key2").setValue("value2").build(), iterator.next());
    assertEquals(HiveReaderProto.Prop.newBuilder().setKey("key1").setValue("value1").build(), iterator.next());
    assertFalse(iterator.hasNext());
  }
}
