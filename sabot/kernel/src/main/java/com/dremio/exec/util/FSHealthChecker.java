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
package com.dremio.exec.util;

import java.io.IOException;
import java.nio.file.AccessMode;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableSet;

/**
 * File System Plugin health checker
 */
public interface FSHealthChecker {

  Set<String> S3_SCHEME = ImmutableSet.of("s3a", "s3", "s3n", "dremios3");
  Logger logger = LoggerFactory.getLogger(FSHealthChecker.class);

  static Optional<FSHealthChecker> getInstance(Path path, String urlScheme, Configuration fsConf) {
    try {
      org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(path.toURI());
      if (!p.isRoot() && S3_SCHEME.stream().anyMatch(scheme -> urlScheme.toLowerCase(Locale.ROOT).startsWith(scheme))) {
        Class<?> s3FSHealthCheckerClass = Class.forName("com.dremio.plugins.s3.store.S3FSHealthChecker");
        return Optional.of((FSHealthChecker) s3FSHealthCheckerClass.getConstructor(Configuration.class).newInstance(fsConf));
      }
    } catch (Exception e) {
      logger.error("Error while initialising health checker for Scheme " + urlScheme, e);
    }
    return Optional.empty();
  }

  void healthCheck(Path path, Set<AccessMode> mode) throws IOException;
}
