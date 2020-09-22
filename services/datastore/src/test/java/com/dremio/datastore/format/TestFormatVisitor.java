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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.datastore.indexed.doughnut.Doughnut;
import com.dremio.datastore.indexed.doughnut.DoughnutConverter;
import com.dremio.datastore.proto.Dummy;
import com.dremio.datastore.proto.DummyObj;

/**
 * Tests for a FormatVisitor.
 */
public class TestFormatVisitor {
  @SuppressWarnings("unchecked")
  protected <T> FormatVisitorTestImpl.Value<T> getVisitable(Format<T> format) {
    return (FormatVisitorTestImpl.Value<T>) format.apply(FormatVisitorTestImpl.INSTANCE);
  }

  @Test
  public void visitString() {
    assertEquals("{ class = java.lang.String, I am a string, key1name = null, key2name = null, key3name = null } ", getVisitable(Format.ofString()).toString());
  }

  @Test
  public void visitUUID() {
    assertEquals("{ class = java.util.UUID, 79b4881b-9ea3-45a8-bae1-63c274b4a90b, key1name = null, key2name = null, key3name = null } ", getVisitable(Format.ofUUID()).toString());
  }

  @Test
  public void visitBytes() {
    assertEquals("{ class = [B, [0, 1, 2, 3, 4, 5, 0], key1name = null, key2name = null, key3name = null } ", getVisitable(Format.ofBytes()).toString());
  }

  @Test
  public void visitProtostuff() {
    assertEquals("{ class = com.dremio.datastore.proto.DummyObj, no value, key1name = null, key2name = null, key3name = null } ", getVisitable(Format.ofProtostuff(DummyObj.class)).toString());
  }

  @Test
  public void visitProtobuf() {
    assertEquals("{ class = com.dremio.datastore.proto.Dummy$DummyObj, no value, key1name = null, key2name = null, key3name = null } ", getVisitable(Format.ofProtobuf(Dummy.DummyObj.class)).toString());
  }

  @Test
  public void visitWrapped() {
    assertEquals("{ " +
        "class = com.dremio.datastore.format.FormatVisitorTestImpl$Value$WrappedValue, { " +
          "outerClass = com.dremio.datastore.indexed.doughnut.Doughnut,  " +
          "innerClass = { " +
          "class = [B, [0, 1, 2, 3, 4, 5, 0], key1name = null, key2name = null, key3name = null " +
          "} ,  " +
          "converterClass = com.dremio.datastore.indexed.doughnut.DoughnutConverter" +
        "} , " +
        "key1name = null, key2name = null, key3name = null } ",
      getVisitable(Format.wrapped(Doughnut.class, new DoughnutConverter(), Format.ofBytes())).toString());
  }

  @Test
  public void visitCompoundFormatPair() {
    assertEquals(
      "{ class = com.dremio.datastore.format.compound.KeyPair, " +
        "KeyPair{ " +
          "key1={ class = java.lang.String, I am a string, key1name = null, key2name = null, key3name = null } , " +
          "key2={ class = java.lang.String, I am a string, key1name = null, key2name = null, key3name = null } }, " +
        "key1name = value1, key2name = value2, key3name = null } ",
      getVisitable(Format
        .ofCompoundFormat("value1", Format.ofString(), "value2", Format.ofString()))
        .toString());
  }

  @Test
  public void visitCompoundFormatTriple() {
    assertEquals(
      "{ class = com.dremio.datastore.format.compound.KeyTriple, " +
        "KeyTriple{ " +
          "key1={ class = java.lang.String, I am a string, key1name = null, key2name = null, key3name = null } , " +
          "key2={ class = java.lang.String, I am a string, key1name = null, key2name = null, key3name = null } , " +
          "key3={ class = java.lang.String, I am a string, key1name = null, key2name = null, key3name = null } }, " +
        "key1name = value1, key2name = value2, key3name = value3 } ",
      getVisitable(Format
        .ofCompoundFormat("value1", Format.ofString(), "value2", Format.ofString(), "value3", Format.ofString()))
        .toString());
  }

  @Test
  public void visitNestingAll() {
    assertEquals(
      "{ class = com.dremio.datastore.format.compound.KeyTriple, " +
        "KeyTriple{ " +
          "key1={ class = java.lang.String, I am a string, key1name = null, key2name = null, key3name = null } , " +
          "key2={ class = java.util.UUID, 79b4881b-9ea3-45a8-bae1-63c274b4a90b, key1name = null, key2name = null, key3name = null } , " +
          "key3={ class = com.dremio.datastore.format.compound.KeyPair, " +
            "KeyPair{ " +
              "key1={ class = [B, [0, 1, 2, 3, 4, 5, 0], key1name = null, key2name = null, key3name = null } , " +
              "key2={ class = com.dremio.datastore.format.compound.KeyTriple, " +
                "KeyTriple{ " +
                  "key1={ class = com.dremio.datastore.proto.DummyObj, no value, key1name = null, key2name = null, key3name = null } , " +
                  "key2={ class = com.dremio.datastore.proto.Dummy$DummyObj, no value, key1name = null, key2name = null, key3name = null } , " +
                  "key3={ class = java.util.UUID, 79b4881b-9ea3-45a8-bae1-63c274b4a90b, key1name = null, key2name = null, key3name = null } }, " +
                "key1name = L3-Value-1, key2name = L3-Value-2, key3name = L3-Value-3 } }, " +
              "key1name = L2-Value-1, key2name = L2-Value-2, key3name = null } }, " +
            "key1name = L1-Value-1, key2name = L1-Value-2, key3name = L1-Value-3 } ",
    getVisitable(
        Format.ofCompoundFormat(
          "L1-Value-1",
          Format.ofString(),
          "L1-Value-2",
          Format.ofUUID(),
          "L1-Value-3",
          Format.ofCompoundFormat(
              "L2-Value-1",
              Format.ofBytes(),
              "L2-Value-2",
              Format.ofCompoundFormat(
                "L3-Value-1",
                Format.ofProtostuff(DummyObj.class),
                "L3-Value-2",
                Format.ofProtobuf(Dummy.DummyObj.class),
                "L3-Value-3",
                Format.ofUUID()))))
        .toString());
  }
}
