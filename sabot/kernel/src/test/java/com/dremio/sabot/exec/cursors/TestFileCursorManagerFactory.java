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
package com.dremio.sabot.exec.cursors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class TestFileCursorManagerFactory {
  @Test
  public void simple() {
    FileCursorManagerFactoryImpl factory = new FileCursorManagerFactoryImpl();

    FileCursorManager mgr1 = factory.getManager("test");
    assertNotNull(mgr1);

    FileCursorManager mgr2 = factory.getManager("test");
    assertEquals(mgr1, mgr2);

    FileCursorManager mgr3 = factory.getManager("testOther");
    assertNotNull(mgr3);
    assertNotEquals(mgr1, mgr3);

    FileCursorManager mgr4 = factory.getManager("testOther");
    assertEquals(mgr3, mgr4);
  }
}
