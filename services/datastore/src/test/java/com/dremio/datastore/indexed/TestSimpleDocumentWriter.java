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
package com.dremio.datastore.indexed;

import org.apache.lucene.document.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for SimpleDocumentWriter
 */
public class TestSimpleDocumentWriter {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testMultiString() {
    IndexKey testKey = IndexKey.newBuilder("test", "TEST", String.class)
      .build();

    IndexKey testKeyMultiple = IndexKey.newBuilder("testmulti","TEST_MULTI", String.class)
      .setCanContainMultipleValues(true)
      .build();

    Document doc = new Document();
    SimpleDocumentWriter documentWriter = new SimpleDocumentWriter(doc);

    documentWriter.write(testKeyMultiple, "foo");
    documentWriter.write(testKeyMultiple, "bar");
    documentWriter.write(testKeyMultiple, "baz", "zap");

    documentWriter.write(testKey, "foo");
    thrown.expect(IllegalStateException.class);
    documentWriter.write(testKey, "bar");
  }

  @Test
  public void testMultiStringList() {
    IndexKey testKey = IndexKey.newBuilder("test", "TEST", String.class)
      .build();

    IndexKey testKeyMultiple = IndexKey.newBuilder("testmulti","TEST_MULTI", String.class)
      .setCanContainMultipleValues(true)
      .build();

    Document doc = new Document();
    SimpleDocumentWriter documentWriter = new SimpleDocumentWriter(doc);

    documentWriter.write(testKeyMultiple, "baz", "zap");

    thrown.expect(IllegalStateException.class);
    documentWriter.write(testKey, "foo", "bar");
  }

  @Test
  public void testMultiNumeric() {
    IndexKey testKey = IndexKey.newBuilder("test", "TEST", Integer.class)
      .build();

    IndexKey testKeyMultiple = IndexKey.newBuilder("testmulti","TEST_MULTI", Integer.class)
      .setCanContainMultipleValues(true)
      .build();

    Document doc = new Document();
    SimpleDocumentWriter documentWriter = new SimpleDocumentWriter(doc);

    documentWriter.write(testKeyMultiple, 1);
    documentWriter.write(testKeyMultiple, 2);

    documentWriter.write(testKey, 1);
    thrown.expect(IllegalStateException.class);
    documentWriter.write(testKey, 2);
  }
}
