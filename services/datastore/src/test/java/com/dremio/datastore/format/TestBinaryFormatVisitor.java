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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.datastore.FormatVisitor;
import com.dremio.datastore.format.visitor.BinaryFormatVisitor;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;
import com.dremio.datastore.proto.Dummy;
import com.dremio.datastore.proto.DummyObj;

/**
 * Tests BinaryFormatVisitor.
 */
@RunWith(Parameterized.class)
public class TestBinaryFormatVisitor<T> {
  @Parameterized.Parameters
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][]{
      {Format.ofString(), false},
      {Format.ofBytes(), true},
      {Format.ofUUID(), false},
      {Format.ofProtostuff(DummyObj.class), false},
      {Format.ofProtobuf(Dummy.DummyObj.class), false},
      {Format.ofCompoundFormat("key1", Format.ofBytes(), "key2", Format.ofString()), true},
      {Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofBytes(), "key3", Format.ofUUID()), true},
      {Format.ofCompoundFormat("key1", Format.ofUUID(), "key2", Format.ofString()), false},
      {Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofString(), "key3", Format.ofUUID()), false},
      {Format.ofCompoundFormat(
        "key1", Format.ofUUID(),
        "key2", Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofBytes())),
        true},
      {Format.ofCompoundFormat(
        "key1", Format.ofString(),
        "key2", Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofString()),
        "key3", Format.wrapped(ByteContainerStoreGenerator.ByteContainer.class, ByteContainerStoreGenerator.ByteContainer::getBytes, ByteContainerStoreGenerator.ByteContainer::new, Format.ofBytes())),
        true},
      {Format.ofCompoundFormat(
        "key1", Format.ofString(),
        "key2", Format.ofCompoundFormat("key1", Format.ofBytes(), "key2", Format.ofString()),
        "key3", Format.ofUUID()),
        true},
      {Format.ofCompoundFormat(
        "key1", Format.ofString(),
        "key2", Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofString()),
        "key3", Format.ofUUID()),
        false},
      {Format.wrapped(ByteContainerStoreGenerator.ByteContainer.class, ByteContainerStoreGenerator.ByteContainer::getBytes, ByteContainerStoreGenerator.ByteContainer::new, Format.ofBytes()), true}
    });
  }

  private final Format<T> format;
  private final boolean expected;
  private final FormatVisitor<Boolean> visitor = new BinaryFormatVisitor();

  public TestBinaryFormatVisitor(Format<T> format, boolean expected) {
    this.format = format;
    this.expected = expected;
  }

  @Test
  public void testBinaryVisitorWithFormat() {
    assertEquals(expected, format.apply(visitor));
  }
}
