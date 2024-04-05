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
package com.dremio.datastore.format.visitor;

import static org.junit.Assert.assertEquals;

import com.dremio.datastore.format.Format;
import com.dremio.datastore.format.WrappedCompoundFormats;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;
import com.dremio.datastore.proto.Dummy;
import com.dremio.datastore.proto.DummyObj;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests CompoundFormatVisitor. */
@RunWith(Parameterized.class)
public class TestCompoundFormatVisitor<T> {
  @Parameterized.Parameters
  public static Collection<Object[]> input() {
    return Arrays.asList(
        new Object[][] {
          {Format.ofString(), false},
          {Format.ofBytes(), false},
          {Format.ofUUID(), false},
          {Format.ofProtostuff(DummyObj.class), false},
          {Format.ofProtobuf(Dummy.DummyObj.class), false},
          {Format.ofCompoundFormat("key1", Format.ofBytes(), "key2", Format.ofString()), true},
          {
            Format.ofCompoundFormat(
                "key1", Format.ofString(), "key2", Format.ofBytes(), "key3", Format.ofUUID()),
            true
          },
          {Format.ofCompoundFormat("key1", Format.ofUUID(), "key2", Format.ofString()), true},
          {
            Format.ofCompoundFormat(
                "key1", Format.ofString(), "key2", Format.ofString(), "key3", Format.ofUUID()),
            true
          },
          {
            Format.ofCompoundFormat(
                "key1", Format.ofUUID(),
                "key2",
                    Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofBytes())),
            true
          },
          {
            Format.ofCompoundFormat(
                "key1", Format.ofString(),
                "key2",
                    Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofString()),
                "key3",
                    Format.wrapped(
                        ByteContainerStoreGenerator.ByteContainer.class,
                        ByteContainerStoreGenerator.ByteContainer::getBytes,
                        ByteContainerStoreGenerator.ByteContainer::new,
                        Format.ofBytes())),
            true
          },
          {
            Format.ofCompoundFormat(
                "key1", Format.ofString(),
                "key2",
                    Format.ofCompoundFormat("key1", Format.ofBytes(), "key2", Format.ofString()),
                "key3", Format.ofUUID()),
            true
          },
          {
            Format.ofCompoundFormat(
                "key1", Format.ofString(),
                "key2",
                    Format.ofCompoundFormat("key1", Format.ofString(), "key2", Format.ofString()),
                "key3", Format.ofUUID()),
            true
          },
          {
            Format.wrapped(
                ByteContainerStoreGenerator.ByteContainer.class,
                ByteContainerStoreGenerator.ByteContainer::getBytes,
                ByteContainerStoreGenerator.ByteContainer::new,
                Format.ofBytes()),
            false
          },
          {
            Format.wrapped(
                WrappedCompoundFormats.KeyPairBytesContainer.class,
                WrappedCompoundFormats.KeyPairBytesContainer::getContainedObject,
                WrappedCompoundFormats.KeyPairBytesContainer::new,
                Format.ofCompoundFormat("k1", Format.ofBytes(), "k2", Format.ofBytes())),
            true
          },
          {
            Format.wrapped(
                WrappedCompoundFormats.KeyTripleBytesContainer.class,
                WrappedCompoundFormats.KeyTripleBytesContainer::getContainedObject,
                WrappedCompoundFormats.KeyTripleBytesContainer::new,
                Format.ofCompoundFormat(
                    "k1", Format.ofBytes(), "k2", Format.ofBytes(), "k3", Format.ofBytes())),
            true
          },
          {
            Format.wrapped(
                WrappedCompoundFormats.KeyPairStringContainer.class,
                WrappedCompoundFormats.KeyPairStringContainer::getContainedObject,
                WrappedCompoundFormats.KeyPairStringContainer::new,
                Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString())),
            true
          },
          {
            Format.wrapped(
                WrappedCompoundFormats.KeyTripleStringContainer.class,
                WrappedCompoundFormats.KeyTripleStringContainer::getContainedObject,
                WrappedCompoundFormats.KeyTripleStringContainer::new,
                Format.ofCompoundFormat(
                    "k1", Format.ofString(), "k2", Format.ofString(), "k3", Format.ofString())),
            true
          },
        });
  }

  private final Format<T> format;
  private final boolean expected;

  public TestCompoundFormatVisitor(Format<T> format, boolean expected) {
    this.format = format;
    this.expected = expected;
  }

  @Test
  public void testBinaryVisitorWithFormat() {
    assertEquals(expected, format.apply(CompoundFormatVisitor.INSTANCE));
  }
}
