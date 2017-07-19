/*
 * Copyright (C) 2017 Dremio Corporation
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import com.dremio.dac.explore.DatasetVersionResource;

/**
 * Test doc generation
 */
public class TestDoc {

  @Test
  public void test() throws Exception {
    Doc docv2 = new Doc(asList(DatasetVersionResource.class));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    docv2.generateDoc("V2", out);
    assertTrue(baos.toString(), baos.toString().contains(DatasetVersionResource.class.getName()));
  }

}
