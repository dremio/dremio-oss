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
package com.dremio.service;

import static org.junit.Assert.fail;

import java.util.Objects;

import org.junit.Test;

/**
 * ServiceRegistry tests.
 */
public class TestServiceRegistry {

  // fake service class
  private static final class TestService implements Service {
    private final String key;

    private TestService(String key) {
      this.key = key;
    }
    @Override
    public void close() {
    }

    @Override
    public void start() {
    }

    @Override
    public String toString() {
      return "TestService [key=" + key + "]";
    }

    @Override
    public int hashCode() {
      return Objects.hash(key);
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof TestService)) {
        return false;
      }
      TestService other = (TestService) obj;
      return Objects.equals(key, other.key);
    }
  }

  @Test
  public void testReplace() throws Exception {
    doTestReplace(false);
  }

  @Test
  public void testReplaceWithTimerEnabled() throws Exception {
    doTestReplace(true);
  }

  public void doTestReplace(boolean withTimerEnabled) throws Exception {
    TestService serviceA = new TestService("a");
    TestService serviceB = new TestService("b");
    TestService newServiceA = new TestService("a");

    try (final ServiceRegistry registry = new ServiceRegistry(withTimerEnabled)) {
      registry.register(serviceA);

      // Check replace
      registry.replace(newServiceA);
      try {
        registry.replace(serviceB);
        fail();
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }
}
