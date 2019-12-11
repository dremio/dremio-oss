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
package com.dremio.telemetry.api.config;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.dremio.test.DremioTest;

/**
 * Ensure include/exclude filter works.
 */
public class TestIncludesExcludesFilter extends DremioTest {

  @Test
  public void onlyExclude() {
    IncludesExcludesFilter f = new IncludesExcludesFilter(Arrays.asList(), Arrays.asList("a.*"));
    assertFalse(f.matches("alpha", null));
    assertTrue(f.matches("beta", null));
  }

  @Test
  public void onlyInclude() {
    IncludesExcludesFilter f = new IncludesExcludesFilter(Arrays.asList("a.*"), Arrays.asList());
    assertTrue(f.matches("alpha", null));
    assertFalse(f.matches("beta", null));
  }

  @Test
  public void includeAndExclude() {
    IncludesExcludesFilter f = new IncludesExcludesFilter(Arrays.asList("a.*"), Arrays.asList("a\\.b.*"));
    assertTrue(f.matches("a.alpha", null));
    assertFalse(f.matches("a.beta", null));
  }

  @Test
  public void noValues() {
    IncludesExcludesFilter f = new IncludesExcludesFilter(Arrays.asList(), Arrays.asList());
    assertTrue(f.matches(null, null));
  }
}
