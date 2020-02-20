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
package com.dremio.dac.server.liveness;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for ClasspathHealthMonitor
 */
public class TestClasspathHealthMonitor {
  private ClasspathHealthMonitor classpathHealthMonitor;
  private String firstJarFolder;
  private String secondJarFolder;

  @Before
  public void setup() throws Exception {
    firstJarFolder = Files.createTempDirectory("abc").toString();
    secondJarFolder = Files.createTempDirectory("xyz").toString();
    Set<String> jarFolders = new HashSet<String>();
    jarFolders.add(firstJarFolder);
    jarFolders.add(secondJarFolder);
    classpathHealthMonitor = new ClasspathHealthMonitor(jarFolders);
  }

  @After
  public void cleanup() {
    new File(firstJarFolder).delete();
    new File(secondJarFolder).delete();
  }

  @Test
  public void testIsHealthy() {
    assertTrue(classpathHealthMonitor.isHealthy());
    new File(secondJarFolder).setExecutable(false);
    assertFalse(classpathHealthMonitor.isHealthy());
    new File(secondJarFolder).setExecutable(true);
    assertTrue(classpathHealthMonitor.isHealthy());
    new File(secondJarFolder).delete();
    assertFalse(classpathHealthMonitor.isHealthy());
  }
}
