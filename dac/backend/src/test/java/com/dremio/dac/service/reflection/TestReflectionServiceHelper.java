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
package com.dremio.dac.service.reflection;

import static com.dremio.exec.catalog.CatalogOptions.REFLECTION_VERSIONED_SOURCE_ENABLED;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.options.OptionManager;

/**
 * Unit Test class for {@link ReflectionServiceHelper}
 */
public class TestReflectionServiceHelper {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private OptionManager optionManager;

  private ReflectionServiceHelper reflectionServiceHelper;

  @Before
  public void setup() {
    reflectionServiceHelper = new ReflectionServiceHelper(null, null, optionManager);
  }

  @Test
  public void testIsVersionedSourceEnabledForVersionedSourceThrowsUnsupportedError() {
    String datasetId = "{\"tableKey\":[\"nessie_without_auth\",\"test\"],\"contentId\":\"cf3c730a-98c0-43a1-855d-02fb97a046c6" +
      "\",\"versionContext\":{\"type\":\"BRANCH\",\"value\":\"main\"}}";
    assertThatThrownBy(() -> reflectionServiceHelper.isVersionedSourceEnabled(datasetId))
      .hasMessageContaining("does not support reflection")
      .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testIsVersionedSourceEnabledForVersionedSource() {
    String datasetId = "{\"tableKey\":[\"nessie_without_auth\",\"test\"],\"contentId\":\"cf3c730a-98c0-43a1-855d-02fb97a046c6" +
      "\",\"versionContext\":{\"type\":\"BRANCH\",\"value\":\"main\"}}";
    when(optionManager.getOption(REFLECTION_VERSIONED_SOURCE_ENABLED)).thenReturn(true);
    assertDoesNotThrow(() -> reflectionServiceHelper.isVersionedSourceEnabled(datasetId));
  }

  @Test
  public void testIsVersionedSourceEnabledForNonVersionedSource() {
    String datasetId = UUID.randomUUID().toString();
    assertDoesNotThrow(() -> reflectionServiceHelper.isVersionedSourceEnabled(datasetId));
  }
}
