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
package com.dremio.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Test;

/**
 * Tests for {@link RequestContextWrapper}
 */
public class TestRequestContextWrapper {
  private static final String JOB_ID = UUID.randomUUID().toString();
  private static final JobIdContext JOB_ID_CTX = new JobIdContext(JOB_ID);
  private static final RequestContext REQ_CTX_WITH_JOB = RequestContext.current().with(JobIdContext.CTX_KEY, JOB_ID_CTX);

  @Test
  public void testRequestContextWrapper() {
    TestApi wrappedApi = RequestContextWrapper.wrapWithContext(REQ_CTX_WITH_JOB, new TestApiImpl(),
        TestApi.class);
    assertThat(RequestContext.current().get(JobIdContext.CTX_KEY)).isNull();

    assertThat(wrappedApi.getJobIdFromContext()).isEqualTo(JOB_ID);
    assertThat(wrappedApi.subApiOfSameType().getJobIdFromContext()).isEqualTo(JOB_ID);
    assertDoesNotThrow(() -> wrappedApi.noReturnValue(JOB_ID));
    assertThatThrownBy(() -> wrappedApi.testException(JOB_ID))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("Expected exception");

    assertThat(RequestContext.current().get(JobIdContext.CTX_KEY)).isNull();
  }

  @Test
  public void testNullReturns() {
    TestApi wrappedApi = RequestContextWrapper.wrapWithContext(REQ_CTX_WITH_JOB, new TestApiImpl(),
        TestApi.class);
    assertThat(wrappedApi.getNullable(JOB_ID)).isNull();
  }

  @Test
  public void testRequestContextInSubApi() {
    TestApi wrappedApi = RequestContextWrapper.wrapWithContext(REQ_CTX_WITH_JOB, new TestApiImpl(),
        TestApi.class);
    TestSubApi subApi = wrappedApi.subApi();
    assertThat(subApi.getJobIdFromContext()).isEqualTo(JOB_ID);
    assertThat(subApi.getPrimitiveType(JOB_ID)).isEqualTo(1);
  }

  @Test
  public void testNoRequestContextInNonInterfaceTypes() {
    TestApi wrappedApi = RequestContextWrapper.wrapWithContext(REQ_CTX_WITH_JOB, new TestApiImpl(),
        TestApi.class);

    NonInterfaceSubApi nonInterfaceSubApi = wrappedApi.nonInterfaceSubApi();
    assertThat(nonInterfaceSubApi.getJobIdFromContext()).isEqualTo(Optional.empty());

    TestApiExtension wrappedApiExt = RequestContextWrapper.wrapWithContext(REQ_CTX_WITH_JOB, new TestApiImpl(), TestApiExtension.class);
    assertThat(RequestContext.current().get(JobIdContext.CTX_KEY)).isNull();
    assertThat(wrappedApiExt.getJobIdFromContext()).isEqualTo(JOB_ID);
  }

  @Test
  public void testStream() {
    TestApi wrappedApi = RequestContextWrapper.wrapWithContext(REQ_CTX_WITH_JOB, new TestApiImpl(),
        TestApi.class);

    Stream<String> jobIds = wrappedApi.subApi().getJobIdContexts();
    assertThat(jobIds).containsOnly(JOB_ID);
  }

  private interface TestApiExtension extends TestApi {
  }

  private interface TestApi {
    String getJobIdFromContext();
    TestSubApi subApi();
    Object getNullable(String expectedJobId);
    void noReturnValue(String expectedJobId);
    String testException(String expectedJobId);
    TestApi subApiOfSameType();
    NonInterfaceSubApi nonInterfaceSubApi();
  }

  private interface TestSubApi{
    String getJobIdFromContext();
    int getPrimitiveType(String expectedJobId);
    Stream<String> getJobIdContexts();
  }

  private static final class TestApiImpl implements TestApiExtension {
    @Override
    public String getJobIdFromContext() {
      return RequestContext.current().get(JobIdContext.CTX_KEY).getJobId();
    }

    @Override
    public TestSubApi subApi() {
      return new TestSubApiImpl();
    }

    @Override
    public Object getNullable(String expectedJobId) {
      assertThat(expectedJobId).isEqualTo(RequestContext.current().get(JobIdContext.CTX_KEY).getJobId());
      return null;
    }

    @Override
    public void noReturnValue(String expectedJobId) {
      assertThat(expectedJobId).isEqualTo(RequestContext.current().get(JobIdContext.CTX_KEY).getJobId());
    }

    @Override
    public String testException(String expectedJobId) {
      assertThat(expectedJobId).isEqualTo(RequestContext.current().get(JobIdContext.CTX_KEY).getJobId());
      throw new IllegalArgumentException("Expected exception");
    }

    @Override
    public TestApi subApiOfSameType() {
      return new TestApiImpl();
    }

    @Override
    public NonInterfaceSubApi nonInterfaceSubApi() {
      return new NonInterfaceSubApi();
    }
  }

  private static final class TestSubApiImpl implements TestSubApi {
    @Override
    public String getJobIdFromContext() {
      return RequestContext.current().get(JobIdContext.CTX_KEY).getJobId();
    }

    @Override
    public int getPrimitiveType(String expectedJobId) {
      assertThat(expectedJobId).isEqualTo(RequestContext.current().get(JobIdContext.CTX_KEY).getJobId());
      return 1;
    }

    @Override
    public Stream<String> getJobIdContexts() {
      return IntStream.range(0, 10).mapToObj(i -> RequestContext.current().get(JobIdContext.CTX_KEY).getJobId());
    }
  }

  private static final class NonInterfaceSubApi {
    public Optional<String> getJobIdFromContext() {
      return Optional.ofNullable(RequestContext.current().get(JobIdContext.CTX_KEY)).map(JobIdContext::getJobId);
    }
  }
}
