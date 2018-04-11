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
package com.dremio.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
    private Long version;
    public Value(Long version) {
      super();
      this.version = version;
    }
    public Long getVersion() {
      return version;
    }
    public void setVersion(Long version) {
      this.version = version;
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
      return v.version == null? Longs.toByteArray(-1) : Longs.toByteArray(v.version);
    }

    @Override
    public Value revert(byte[] v) {
      long version = Longs.fromByteArray(v);
      return new Value(version == -1 ? null : version);
    }
  }

  private static final class ValueVersionExtractor implements VersionExtractor<Value> {
    @Override
    public void setVersion(Value value, Long version) {
      value.setVersion(version);
    }

    @Override
    public Long getVersion(Value value) {
      return value.getVersion();
    }

    @Override
    public Long incrementVersion(Value value) {
      Long v = value.getVersion();
      value.setVersion(v == null ? 0L : v + 1);
      return v;
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
    s.put("a", new Value(0L));
  }

  @Test
  public void testCreate() {
    s.put("a", new Value(null));
  }

  @Test
  public void testUpdate() {
    Value v = new Value(null);
    s.put("a", v);
    assertEquals(0L, v.getVersion().longValue());
    s.put("a", v);
    assertEquals(1L, v.getVersion().longValue());
    s.put("a", v);
    assertEquals(2L, v.getVersion().longValue());
  }

  @Test
  public void testConcurrentUpdate() {
    Value v0 = new Value(null);
    s.put("a", v0);
    Value v1 = new Value(v0.getVersion());
    assertEquals(0L, v0.getVersion().longValue());
    s.put("a", v0);
    assertEquals(1L, v0.getVersion().longValue());

    exception.expect(ConcurrentModificationException.class);
    s.put("a", v1);
  }

  @Test
  public void testDelete() {
    Value v = new Value(null);
    s.put("a", v);
    assertEquals(0L, v.getVersion().longValue());
    s.delete("a", v.getVersion());
    assertFalse(s.contains("a"));
  }

  @Test
  public void testDeleteBadVersion() {
    Value v0 = new Value(null);
    s.put("a", v0);
    assertEquals(0L, v0.getVersion().longValue());
    Value v1 = new Value(v0.getVersion());
    s.put("a", v1);
    assertEquals(1L, v1.getVersion().longValue());

    exception.expect(ConcurrentModificationException.class);
    s.delete("a", v0.getVersion());
  }

}
