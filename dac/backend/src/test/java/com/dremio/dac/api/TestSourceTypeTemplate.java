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

import com.dremio.exec.catalog.conf.DoNotDisplay;
import com.dremio.exec.catalog.conf.SourceType;

import io.protostuff.Tag;

/**
 * Test class for {@link SourceTypeTemplate}
 */
public class TestSourceTypeTemplate {
  @Test
  public void testPropertyOverride() {
    SourceTypeTemplate classATemplate = SourceTypeTemplate.fromSourceClass(ClassA.class, true);
    Assert.assertEquals(3, classATemplate.getElements().size());
    SourceTypeTemplate classBTemplate = SourceTypeTemplate.fromSourceClass(ClassB.class, true);
    // one element is hidden
    Assert.assertEquals(3, classBTemplate.getElements().size());
    SourceTypeTemplate classCTemplate = SourceTypeTemplate.fromSourceClass(ClassC.class, true);
    // the sub-subclass unhid the field
    Assert.assertEquals(4, classCTemplate.getElements().size());

  }

  @SourceType(value = "ClassA")
  private static class ClassA {

    @Tag(1)
    public int intField;

    @Tag(2)
    public boolean booleanField;

    @Tag(3)
    public String field;

    public ClassA() {
    }
  }

  @SourceType(value = "ClassB")
  private static class ClassB extends ClassA {
    @Tag(2)
    @DoNotDisplay
    public boolean booleanField;

    @Tag(4)
    public double field;

    public ClassB() {
    }
  }

  @SourceType(value = "ClassC")
  private static class ClassC extends ClassB {
    @Tag(2)
    public boolean booleanField;

    @Tag(3)
    public String field;

    public ClassC() {
    }
  }
}
