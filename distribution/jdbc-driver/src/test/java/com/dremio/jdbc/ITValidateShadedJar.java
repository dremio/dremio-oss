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
package com.dremio.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

/**
 * Test to check the content of the JDBC driver jar has been shaded properly.
 * Except for com.dremio.jdbc and org.slf4j classes, everything else should be under cdjd.
 * (stands for for com.dremio.jdbc driver) namespace.
 */
public class ITValidateShadedJar {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ITValidateShadedJar.class);

  private static File getJdbcFile() throws MalformedURLException {
    return new File(
        String.format("%s../../target/dremio-jdbc-driver-%s.jar",
            ClassLoader.getSystemClassLoader().getResource("").getFile(),
            System.getProperty("project.version")
            ));
  }

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = Timeout.builder().withTimeout(2, TimeUnit.MINUTES).build();

  private static final List<String> ALLOWED_PREFIXES = Collections.unmodifiableList(Arrays.asList(
      "com/dremio/jdbc/",
      "cdjd/",
      "org/slf4j/",
      "META-INF/"));

  private static final List<String> ALLOWED_FILES = Collections.unmodifiableList(Arrays.asList(
      "CDJDLog4j-charsets.properties",
      "git.properties",
      "arrow-git.properties",
      "dremio-jdbc.properties",
      "dremio-reference.conf",
      "sabot-default.conf",
      "sabot-module.conf"));

  @Test
  public void validateShadedJar() throws IOException {
    // Validate the content of the jar to enforce all 3rd party dependencies have been shaded
    try (JarFile jar = new JarFile(getJdbcFile())) {
      toIterator(jar.entries()).forEachRemaining(entry -> {
        if (entry.isDirectory()) {
          // skip directories
          return;
        }
        assertThat(entry.getName())
          .satisfiesAnyOf(
            name -> assertThat(ALLOWED_PREFIXES).anySatisfy(x -> assertThat(name).startsWith(x)),
            name -> assertThat(ALLOWED_FILES).anySatisfy(x -> assertThat(name).isEqualTo(x)));
      });
    }
  }

  private static <E> Iterator<E> toIterator(Enumeration<E> enumeration) {
    return new Iterator<E>() {
      @Override
      public boolean hasNext() {
        return enumeration.hasMoreElements();
      }

      @Override
      public E next() {
        return enumeration.nextElement();
      }
    };
  }
}
