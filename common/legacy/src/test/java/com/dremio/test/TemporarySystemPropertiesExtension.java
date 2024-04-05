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
package com.dremio.test;

import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TemporarySystemPropertiesExtension implements BeforeEachCallback, AfterEachCallback {
  private final String storeKey = UUID.randomUUID().toString();

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Properties originalProperties = (Properties) System.getProperties().clone();
    storeProperties(context, originalProperties);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    Properties originalProperties = retrieveProperties(context);
    Objects.requireNonNull(
        originalProperties, "Original Properties not found in ExtensionContext store");

    restoreSystemProperties(originalProperties);
  }

  private void storeProperties(ExtensionContext context, Properties properties) {
    context.getStore(ExtensionContext.Namespace.create(storeKey)).put(Properties.class, properties);
  }

  private Properties retrieveProperties(ExtensionContext context) {
    return context
        .getStore(ExtensionContext.Namespace.create(storeKey))
        .get(Properties.class, Properties.class);
  }

  private void restoreSystemProperties(Properties originalProperties) {
    System.setProperties(originalProperties);
  }

  public void setSystemProperty(String key, String value) {
    System.setProperty(key, value);
  }
}
