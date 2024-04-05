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

import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.NEW_FILES_GRACE_PERIOD;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.PER_REF_CUTOFF_POLICIES_ERROR_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.assertj.core.data.Percentage;
import org.junit.Test;
import org.projectnessie.client.api.GetRepositoryConfigBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.rest.NessieServiceException;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.model.GarbageCollectorConfig;
import org.projectnessie.model.ImmutableGarbageCollectorConfig;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.RepositoryConfigResponse;

/** Test NessieGCPolicy for Vacuum Catalog command. */
public class TestNessieGCPolicy {

  @Test
  public void testNessieGCPolicyWithTimestamp() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder()
            .defaultCutoffPolicy("PT10S")
            .newFilesGracePeriod(Duration.ofDays(1))
            .build();
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.singletonList(gcConfig));
    NessieGCPolicy nessieGCPolicy = new NessieGCPolicy(nessieApiV2, 0);
    assertThat(nessieGCPolicy.getOlderThanInMillis())
        .isCloseTo(System.currentTimeMillis() - 10_000, Percentage.withPercentage(10));
    assertThat(nessieGCPolicy.getGracePeriodInMillis()).isEqualTo(24 * 60 * 60 * 1000L);
    assertThat(nessieGCPolicy.getRetainLast()).isEqualTo(1);
  }

  @Test
  public void testNessieGCPolicyWithInstantTime() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    Instant instantTime = Instant.now();
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder()
            .defaultCutoffPolicy(instantTime.toString())
            .build();
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.singletonList(gcConfig));
    NessieGCPolicy nessieGCPolicy = new NessieGCPolicy(nessieApiV2, 0);
    assertThat(nessieGCPolicy.getOlderThanInMillis()).isEqualTo(instantTime.toEpochMilli());
  }

  @Test
  public void testNessieGCPolicyWithDefault() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.emptyList());
    NessieGCPolicy nessieGCPolicy = new NessieGCPolicy(nessieApiV2, 2L);
    assertThat(nessieGCPolicy.getOlderThanInMillis())
        .isCloseTo(
            System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2L),
            Percentage.withPercentage(10));
    assertThat(nessieGCPolicy.getRetainLast()).isEqualTo(1);
  }

  @Test
  public void testNessieGCPolicyWithCommits() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("30").build();
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.singletonList(gcConfig));
    assertThatThrownBy(() -> new NessieGCPolicy(nessieApiV2, 0))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE);
  }

  @Test
  public void testNessieGCPolicyWithNoneCutOffPolicy() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("NONE").build();
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.singletonList(gcConfig));
    NessieGCPolicy nessieGCPolicy = new NessieGCPolicy(nessieApiV2, 0);
    assertThat(nessieGCPolicy.getOlderThanInMillis()).isEqualTo(0L);
  }

  @Test
  public void testNessieGCPolicyWithMultipleViolations() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder()
            .defaultCutoffPolicy("30")
            .newFilesGracePeriod(Duration.ofMinutes(10))
            .build();
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.singletonList(gcConfig));
    assertThatThrownBy(() -> new NessieGCPolicy(nessieApiV2, 0))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE);
    assertThatThrownBy(() -> new NessieGCPolicy(nessieApiV2, 0))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(NEW_FILES_GRACE_PERIOD);
  }

  @Test
  public void testNessieGCPolicyWithPerRefCutOffPolicy() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder()
            .defaultCutoffPolicy("PT0S")
            .perRefCutoffPolicies(
                Collections.singletonList(
                    GarbageCollectorConfig.ReferenceCutoffPolicy.referenceCutoffPolicy("", "")))
            .build();
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.singletonList(gcConfig));
    assertThatThrownBy(() -> new NessieGCPolicy(nessieApiV2, 0))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(PER_REF_CUTOFF_POLICIES_ERROR_MESSAGE);
  }

  @Test
  public void testNessieGCPolicyWithNullCutOffPolicy() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    RepositoryConfigResponse repositoryConfigResponse = mock(RepositoryConfigResponse.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get()).thenReturn(repositoryConfigResponse);
    ImmutableGarbageCollectorConfig gcConfig = ImmutableGarbageCollectorConfig.builder().build();
    when(repositoryConfigResponse.getConfigs()).thenReturn(Collections.singletonList(gcConfig));
    assertThatThrownBy(() -> new NessieGCPolicy(nessieApiV2, 0))
        .hasMessageContaining(DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE);
  }

  @Test
  public void testNessieGCPolicyOlderVersion() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get())
        .thenThrow(
            new NessieServiceException(
                ImmutableNessieError.builder()
                    .status(404)
                    .reason("Not Found")
                    .message("Not Found (HTTP/404)")
                    .build()));
    NessieGCPolicy nessieGCPolicy = new NessieGCPolicy(nessieApiV2, 2L);
    assertThat(nessieGCPolicy.getOlderThanInMillis())
        .isCloseTo(
            System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2L),
            Percentage.withPercentage(10));
  }

  @Test
  public void testNessieGCPolicyOlderDBModel() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get())
        .thenThrow(
            new NessieBadRequestException(
                ImmutableNessieError.builder()
                    .status(400)
                    .reason("Bad Request")
                    .message(
                        "Bad Request (HTTP/400): Old database model does not support repository config objects")
                    .build()));
    NessieGCPolicy nessieGCPolicy = new NessieGCPolicy(nessieApiV2, 2L);
    assertThat(nessieGCPolicy.getOlderThanInMillis())
        .isCloseTo(
            System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2L),
            Percentage.withPercentage(10));
  }

  @Test
  public void testNessieGCPolicyWithRTE() {
    final NessieApiV2 nessieApiV2 = mock(NessieApiV2.class);
    GetRepositoryConfigBuilder getRepositoryConfigBuilder = mock(GetRepositoryConfigBuilder.class);
    when(nessieApiV2.getRepositoryConfig()).thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.type(RepositoryConfig.Type.GARBAGE_COLLECTOR))
        .thenReturn(getRepositoryConfigBuilder);
    when(getRepositoryConfigBuilder.get())
        .thenThrow(new IllegalArgumentException("Throwable Exception"));
    assertThatThrownBy(() -> new NessieGCPolicy(nessieApiV2, 0))
        .hasMessageContaining("Throwable Exception");
  }
}
