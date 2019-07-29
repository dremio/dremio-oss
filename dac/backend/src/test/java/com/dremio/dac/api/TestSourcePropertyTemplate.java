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
package com.dremio.dac.api;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.test.DremioTest;

/**
 * Test class for {@code SourcePropertyTemplat}
 */
public class TestSourcePropertyTemplate {
  @Test
  public void testAllSourcesCanBeTemplated() throws Exception {
    // make sure each configurable source can be used as a SourceTypeTemplate
    for(Class<?> input : DremioTest.CLASSPATH_SCAN_RESULT.getAnnotatedClasses(SourceType.class)) {
      com.dremio.exec.catalog.conf.SourceType type = input.getAnnotation(SourceType.class);
      if (type.configurable()) {
        SourceTypeTemplate sourceTypeTemplate = SourceTypeTemplate.fromSourceClass(input, true);
        Assert.assertEquals(type.value(), sourceTypeTemplate.getSourceType());
      }
    }
  }

}
