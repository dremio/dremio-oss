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
package com.dremio.exec.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

/**
 * Tests for {@link VacuumOptions}
 */
public class TestVacuumOptions {

  @Test
  public void testVacuumOptionsFromNessieGCPolicy() {
    final NessieGCPolicy nessieGCPolicy = mock(NessieGCPolicy.class);
    when(nessieGCPolicy.getOlderThanInMillis()).thenReturn(100L);
    when(nessieGCPolicy.getRetainLast()).thenReturn(1);
    when(nessieGCPolicy.getGracePeriodInMillis()).thenReturn(0L);
    VacuumOptions vacuumCatalogOptions = new VacuumOptions(nessieGCPolicy);
    assertThat(vacuumCatalogOptions.getOlderThanInMillis()).isEqualTo(100L);
    assertThat(vacuumCatalogOptions.getGracePeriodInMillis()).isEqualTo(0);
  }

}
