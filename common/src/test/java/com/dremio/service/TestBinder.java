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
package com.dremio.service;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;

/**
 * A few basic tests of the binder.
 */
public class TestBinder {

  @Test
  public void ensureChildLookup(){
    BinderImpl bi = new BinderImpl();
    bi.bindSelf(Family.class);
    Binder inner = bi.newChild();
    inner.bindSelf(Children.class);
    Assert.assertTrue(inner.lookup(Family.class).ok());
  }

  /**
   * Test class
   */
  public static class Family {

    private Children children;

    @Inject
    public Family(Children children){
      this.children = children;
    }

    public boolean ok(){
      return children.ok();
    }
  }

  /**
   * Test class
   */
  public static class Children {

    @Inject
    public Children(){}

    public boolean ok(){
      return true;
    }
  }
}
