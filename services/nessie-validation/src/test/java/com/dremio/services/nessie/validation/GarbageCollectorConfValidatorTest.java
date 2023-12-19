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
package com.dremio.services.nessie.validation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.GarbageCollectorConfig;
import org.projectnessie.model.ImmutableGarbageCollectorConfig;
import org.projectnessie.model.ImmutableReferenceCutoffPolicy;

public class GarbageCollectorConfValidatorTest {

  @Test
  public void testCutOffPolicy() {

    GarbageCollectorConfig config = ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("PT10M").build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(config).size()).isEqualTo(0);

    GarbageCollectorConfig configWith24HrsGracePeriod = ImmutableGarbageCollectorConfig.builder().newFilesGracePeriod(Duration.ofDays(1)).defaultCutoffPolicy("PT10M").build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWith24HrsGracePeriod).size()).isEqualTo(0);

    GarbageCollectorConfig configWithInvalidGracePeriodAndInvalidDuration = ImmutableGarbageCollectorConfig.builder().newFilesGracePeriod(Duration.ofMinutes(10)).defaultCutoffPolicy("PT10M").build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWithInvalidGracePeriodAndInvalidDuration).size()).isEqualTo(1);

    GarbageCollectorConfig configWithInvalidDuration = ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("PT10M").build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWithInvalidDuration).size()).isEqualTo(0);

    GarbageCollectorConfig configWithEmptyDefaultPolicy = ImmutableGarbageCollectorConfig.builder().build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWithEmptyDefaultPolicy).size()).isEqualTo(1);

    GarbageCollectorConfig configWithNoneDefaultPolicy = ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("NONE").build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWithNoneDefaultPolicy).size()).isEqualTo(0);


    GarbageCollectorConfig configWithDefaultPolicyAsNoOfCommits = ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("1").build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWithDefaultPolicyAsNoOfCommits).size()).isEqualTo(1);

    GarbageCollectorConfig configWithPerRefCutOffPolicy = ImmutableGarbageCollectorConfig.builder().perRefCutoffPolicies(Collections.singletonList(ImmutableReferenceCutoffPolicy.builder().referenceNamePattern("branch1").policy("branch1").build())).build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWithPerRefCutOffPolicy).size()).isEqualTo(2);

    GarbageCollectorConfig configWithPerRefCutOffPolicyAndDefaultPolicy = ImmutableGarbageCollectorConfig.builder()
      .defaultCutoffPolicy("1")
      .perRefCutoffPolicies(Collections.singletonList(ImmutableReferenceCutoffPolicy.builder().referenceNamePattern("branch1").policy("PT11S").build()))
      .build();
    assertThat(GarbageCollectorConfValidator.validateCompliance(configWithPerRefCutOffPolicyAndDefaultPolicy).size()).isEqualTo(2);

  }
}
