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
package com.dremio.dac.server;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.dremio.common.SentinelSecure;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

/**
 * Ensure SentinelSecure Filter works correctly.
 */
public class TestSentinelSecureFilter {

  private static final String SENTINEL = "mySentinel";

  /**
   * Test object for sentinel secure
   */
  @JsonFilter(SentinelSecure.FILTER_NAME)
  public static class Example {

    @SentinelSecure(SENTINEL)
    private final String secured;
    private final String foo;

    @JsonCreator
    public Example(@JsonProperty("secured") String secured, @JsonProperty("foo") String foo) {
      super();
      this.secured = secured;
      this.foo = foo;
    }

    public String getSecured() {
      return secured;
    }

    public String getFoo() {
      return foo;
    }

  }

  @Test
  public void ensureSecureWithValue() throws IOException {
    ObjectMapper mapper = JSONUtil.mapper();
    Example e1 = new Example("secret", "bar");
    Example e2 = mapper.readValue(mapper.writeValueAsString(e1), Example.class);
    assertEquals(SENTINEL, e2.getSecured());
  }

  @Test
  public void ensureSecureWithNull() throws IOException {
    ObjectMapper mapper = JSONUtil.mapper();
    Example e1 = new Example(null, "bar");
    Example e2 = mapper.readValue(mapper.writeValueAsString(e1), Example.class);
    assertEquals(e1.getSecured(), e2.getSecured());
  }

  @Test
  public void ensureTestOnlyWithNull() throws IOException {
    ObjectMapper mapper = JSONUtil.mapper();
    mapper.setFilterProvider(new SimpleFilterProvider().addFilter(SentinelSecure.FILTER_NAME, SentinelSecureFilter.TEST_ONLY));
    Example e1 = new Example(null, "bar");
    Example e2 = mapper.readValue(mapper.writeValueAsString(e1), Example.class);
    assertEquals(e1.getSecured(), e2.getSecured());
  }

  @Test
  public void ensureTestOnlyWithValue() throws IOException {
    ObjectMapper mapper = JSONUtil.mapper();
    mapper.setFilterProvider(new SimpleFilterProvider().addFilter(SentinelSecure.FILTER_NAME, SentinelSecureFilter.TEST_ONLY));
    Example e1 = new Example("secret", "bar");
    Example e2 = mapper.readValue(mapper.writeValueAsString(e1), Example.class);
    assertEquals(e1.getSecured(), e2.getSecured());
  }

}
