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
package com.dremio.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ConcurrentModificationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;

/**
 * Tests OCCKVStore
 */
public abstract class AbstractTestOCCKVStore {

  private KVStore<String, Value> s;
  private KVStoreProvider p;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private static class Value {
    private String tag;
    private Long version;

    public Value(String tag) {
      super();
      this.tag = tag;
    }

    public String getTag() {
      return tag;
    }

    public void setTag(String tag) {
      this.tag = tag;
    }

    public void setVersion(Long version) {
      this.version = version;
    }

    public Long getVersion() {
      return version;
    }
  }

  private static final class ValueSerializer extends Serializer<Value> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ValueSerializer() {
    }

    @Override
    public String toJson(Value v) throws IOException {
      return objectMapper.writeValueAsString(v);
    }

    @Override
    public Value fromJson(String v) throws IOException {
      return objectMapper.readValue(v, Value.class);
    }

    @Override
    public byte[] convert(Value v) {
      return v.tag == null? Longs.toByteArray(-1) : Longs.toByteArray(Long.valueOf(v.tag));
    }

    @Override
    public Value revert(byte[] v) {
      long version = Longs.fromByteArray(v);
      return new Value(version == -1 ? null : Long.toString(version));
    }
  }

  private static final class ValueVersionExtractor implements VersionExtractor<Value> {
    @Override
    public String getTag(Value value) {
      return value.getTag();
    }

    @Override
    public void setTag(Value value, String tag) {
      value.setTag(tag);
    }

    @Override
    public void setVersion(Value value, Long version) {
      value.setVersion(version);
    }

    @Override
    public Long getVersion(Value value) {
      return value.getVersion();
    }

  }

  abstract KVStoreProvider createKKStoreProvider() throws Exception;

  @SuppressWarnings("resource")
  @Before
  public void before() throws Exception {
    ScanResult r = DremioTest.CLASSPATH_SCAN_RESULT;
    p = createKKStoreProvider();
    s = p.getStore(TestOCCBuilder.class);

  }

  /**
   * Test builder.
   */
  public static class TestOCCBuilder implements StoreCreationFunction<KVStore<String, Value>> {

    @Override
    public KVStore<String, Value> build(StoreBuildingFactory factory) {
      return factory.<String, Value>newStore()
        .name("test-occ")
        .keySerializer(StringSerializer.class)
        .valueSerializer(ValueSerializer.class)
        .versionExtractor(ValueVersionExtractor.class)
        .build();
    }

  }

  @After
  public void after() throws Exception {
    p.close();
  }

  @Test
  public void testMissingPrevious() {
    exception.expect(ConcurrentModificationException.class);
    s.put("a", new Value("0"));
  }

  @Test
  public void testCreate() {
    s.put("a", new Value(null));
  }

  @Test
  public void testUpdate() {
    Value v = new Value(null);
    s.put("a", v);
    assertEquals("0", v.getTag());
    s.put("a", v);
    assertEquals("1", v.getTag());
    s.put("a", v);
    assertEquals("2", v.getTag());
  }

  @Test
  public void testConcurrentUpdate() {
    Value v0 = new Value(null);
    s.put("a", v0);
    Value v1 = new Value(v0.getTag());
    assertEquals("0", v0.getTag());
    s.put("a", v0);
    assertEquals("1", v0.getTag());

    boolean threw = false;
    final String previousVersion = v1.getTag();
    try {
      s.put("a", v1);
    } catch (ConcurrentModificationException e) {
      threw = true;
    }

    assertTrue(threw);

    // ensure that v1 doesn't get mutated if the update fails
    assertEquals(previousVersion, v1.getTag());
  }

  @Test
  public void testDelete() {
    Value v = new Value(null);
    s.put("a", v);
    assertEquals("0", v.getTag());
    s.delete("a", v.getTag());
    assertFalse(s.contains("a"));
  }

  @Test
  public void testDeleteBadVersion() {
    Value v0 = new Value(null);
    s.put("a", v0);
    assertEquals("0", v0.getTag());
    Value v1 = new Value(v0.getTag());
    s.put("a", v1);
    assertEquals("1", v1.getTag());

    exception.expect(ConcurrentModificationException.class);
    s.delete("a", v0.getTag());
  }

  @Test
  public void testOnFlyUpdateToString() {
    //Set only long version and save to store
    Value v0 = new Value(null);
    v0.setVersion(0L);

    KVStoreTuple<Value> tuple = new KVStoreTuple(new ValueSerializer(), new ValueVersionExtractor());
    tuple.setObject(v0);
    assertEquals("0", tuple.getTag());

    // Make sure that the inline upgrade doesn't overwrite an existing tag
    Value v1 = new Value(null);
    v1.setVersion(0L);
    v1.setTag("1");

    KVStoreTuple<Value> tuple1 = new KVStoreTuple(new ValueSerializer(), new ValueVersionExtractor());
    tuple1.setObject(v1);
    assertEquals("1", tuple1.getTag());
  }
}
