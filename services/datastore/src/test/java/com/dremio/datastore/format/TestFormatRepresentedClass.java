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

import java.util.UUID;

import org.junit.Test;

import com.dremio.datastore.format.compound.KeyPair;
import com.dremio.datastore.format.compound.KeyTriple;
import com.dremio.datastore.indexed.doughnut.Doughnut;
import com.dremio.datastore.indexed.doughnut.DoughnutConverter;
import com.dremio.datastore.proto.Dummy;
import com.dremio.datastore.proto.DummyObj;

/**
 * Tests for expected classes returned from {@link Format#getRepresentedClass()}
 */
public class TestFormatRepresentedClass {
  @Test
  public void testCompoundPairFormat() {
    Format format = Format.ofCompoundFormat("value1", Format.ofString(), "value2", Format.ofString());
    assertEquals(KeyPair.class, format.getRepresentedClass());
  }

  @Test
  public void testCompoundTripleFormat() {
    Format format = Format.ofCompoundFormat("value1", Format.ofString(), "value2", Format.ofString(), "value3", Format.ofString());
    assertEquals(KeyTriple.class, format.getRepresentedClass());
  }

  @Test
  public void testProtostuffFormats() {
    Format format = Format.ofProtostuff(DummyObj.class);
    assertEquals(DummyObj.class, format.getRepresentedClass());
  }

  @Test
  public void testProtobufFormat() {
    Format format = Format.ofProtobuf(Dummy.DummyObj.class);
    assertEquals(Dummy.DummyObj.class, format.getRepresentedClass());
  }

  @Test
  public void testBytesFormat() {
    Format format = Format.ofBytes();
    assertEquals(byte[].class, format.getRepresentedClass());
  }

  @Test
  public void testUUIDFormat() {
    Format format = Format.ofUUID();
    assertEquals(UUID.class, format.getRepresentedClass());
  }

  @Test
  public void testStringFormat() {
    Format format = Format.ofString();
    assertEquals(String.class, format.getRepresentedClass());
  }

  @Test
  public void testWrappedFormat() {
    Format format = Format.wrapped(Doughnut.class, new DoughnutConverter(), Format.ofBytes());
    assertEquals(Doughnut.class, format.getRepresentedClass());
  }
}
