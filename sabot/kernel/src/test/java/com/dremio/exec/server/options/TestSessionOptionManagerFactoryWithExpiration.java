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
package com.dremio.exec.server.options;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.dremio.test.DremioTest;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class TestSessionOptionManagerFactoryWithExpiration {
  private final SessionOptionManagerFactoryWithExpiration factory =
      new SessionOptionManagerFactoryWithExpiration(
          new OptionValidatorListingImpl(DremioTest.CLASSPATH_SCAN_RESULT),
          Duration.ofSeconds(100),
          100);

  @Test
  public void testGetOrCreate() {
    assertThat(factory.getOrCreate("abc")).isNotNull();
    assertThat(factory.contains("abc")).isTrue();
  }

  @Test
  public void testDelete() {
    assertThat(factory.getOrCreate("abc")).isNotNull();
    factory.delete("abc");
    assertThat(factory.contains("abc")).isFalse();
  }
}
