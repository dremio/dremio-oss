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
package com.dremio.test;

import java.util.Properties;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A JUnit rule to set/reset system properties
 *
 * Note that this rule is not thread-safe as system properties are global objects
 */
public class TemporarySystemProperties implements TestRule {
  public TemporarySystemProperties() {
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {

      @Override
      public void evaluate() throws Throwable {
        final Properties cloned = (Properties) System.getProperties().clone();

        try {
          base.evaluate();
        } finally {
          System.setProperties(cloned);
        }

      }
    };
  }

  // helper methods
  /**
   * Get system property for key
   * @param key
   * @return
   */
  public String get(String key) {
    return System.getProperty(key);
  }

  /**
   * Get system property for key, or default if not set
   * @param key
   * @param def
   * @return
   */
  public String get(String key, String def) {
    return System.getProperty(key, def);
  }

  /**
   * Set system property for key
   * @param key
   * @param value
   */
  public void set(String key, String value) {
    System.setProperty(key, value);
  }

  /**
   * Clear system property for key
   * @param name
   */
  public void clear(String key) {
    System.clearProperty(key);
  }

}
