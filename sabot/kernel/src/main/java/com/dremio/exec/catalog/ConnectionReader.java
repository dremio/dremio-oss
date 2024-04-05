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
package com.dremio.exec.catalog;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.conf.AbstractSecretRef;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.service.namespace.AbstractConnectionReader;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Throwables;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/** Resolves concrete ConnectionConf types using Classpath Scanning. */
public interface ConnectionReader extends AbstractConnectionReader {

  ConnectionConf<?, ?> getConnectionConf(SourceConfig config);

  /**
   * Returns the given source config as a string, without secret fields. Useful in error messages
   * and debug logs.
   *
   * @param sourceConfig source config
   * @return source config as string, without secret fields
   */
  String toStringWithoutSecrets(SourceConfig sourceConfig);

  /**
   * Get a map of all the available connection configuration classes
   *
   * <p>The map key is the source type, and the value is the class representing the connection
   * configuration
   *
   * @return an immutable map
   */
  Map<String, Class<? extends ConnectionConf<?, ?>>> getAllConnectionConfs();

  @SuppressWarnings("deprecation")
  static String toType(SourceConfig config) {
    if (config.getType() != null) {
      return config.getType();
    }

    if (config.getLegacySourceTypeEnum() != null) {
      return config.getLegacySourceTypeEnum().name();
    }

    throw new IllegalStateException(
        String.format(
            "Unable to manage source of type: named: [%s], legacy enum: [%d].",
            config.getType(), config.getLegacySourceTypeEnum().getNumber()));
  }

  static ConnectionReader of(final ScanResult scanResult, final SabotConfig sabotConfig) {
    // Register delegates before ConnectionReader schemas are created. Redundant binding for upgrade
    // scenarios.
    AbstractSecretRef.registerDelegates();
    try {
      final Class<? extends ConnectionReader> clazz =
          sabotConfig.getClass(
              "dremio.connection.reader.class", ConnectionReader.class, ConnectionReaderImpl.class);
      return (ConnectionReader)
          clazz.getMethod("makeReader", ScanResult.class).invoke(null, scanResult);
    } catch (final InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      Throwables.throwIfUnchecked(cause);
      throw new RuntimeException("Unable to instantiate ConnectionReader", cause);
    } catch (final ReflectiveOperationException e) {
      throw new RuntimeException("Unable to instantiate ConnectionReader", e);
    }
  }
}
