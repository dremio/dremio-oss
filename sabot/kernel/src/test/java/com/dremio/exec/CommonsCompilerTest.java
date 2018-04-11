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
package com.dremio.exec;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.janino.CompilerFactory;
import org.junit.Test;

/**
 * Test that the default compiler is Janino and not JDK
 * see DX-9678 for more details
 */
public class CommonsCompilerTest {

  @Test
  public void testProperties() throws IOException {
    Properties properties = new Properties();
    try(InputStream is = this.getClass().getClassLoader().getResourceAsStream("org.codehaus.commons.compiler.properties")) {
      properties.load(is);
    }

    assertEquals(CompilerFactory.class.getName(), properties.get("compilerFactory"));
    assertEquals("true", properties.get("test.dremio.please.ignore"));
  }

  @Test
  public void testFactory() throws Exception {
    assertEquals(CompilerFactory.class, CompilerFactoryFactory.getDefaultCompilerFactory().getClass());
  }

}
