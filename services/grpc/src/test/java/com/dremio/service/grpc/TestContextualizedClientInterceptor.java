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
package com.dremio.service.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.context.CatalogContext;
import com.dremio.context.ExecutorToken;
import com.dremio.context.SupportContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Tests for ContextualizedClientInterceptor */
public class TestContextualizedClientInterceptor {

  @Test
  public void buildMultiTenantClientInterceptorDefault() {
    ContextualizedClientInterceptor clientInterceptor =
        ContextualizedClientInterceptor.buildMultiTenantClientInterceptor();

    assertThat(clientInterceptor.getActions().size()).isEqualTo(4);
    assertDefaultContextTransfers(clientInterceptor);
  }

  @Test
  public void buildMultiTenantClientInterceptorParameter() {
    ContextualizedClientInterceptor clientInterceptor =
        ContextualizedClientInterceptor.buildMultiTenantClientInterceptor(
            Collections.singletonList(
                new ContextualizedClientInterceptor.ContextTransferBehavior(
                    ExecutorToken.CTX_KEY, true, null)));

    assertThat(clientInterceptor.getActions().size()).isEqualTo(5);
    assertDefaultContextTransfers(clientInterceptor);
    assertThat(clientInterceptor.getActions())
        .anyMatch(
            action ->
                action.getKey().getName().equals(ExecutorToken.CTX_KEY.getName())
                    && (action.getRequired()));
  }

  private static void assertDefaultContextTransfers(
      ContextualizedClientInterceptor clientInterceptor) {
    assertThat(clientInterceptor.getActions())
        .anyMatch(
            action ->
                action.getKey().getName().equals(TenantContext.CTX_KEY.getName())
                    && (action.getRequired()));
    assertThat(clientInterceptor.getActions())
        .anyMatch(
            action ->
                action.getKey().getName().equals(SupportContext.CTX_KEY.getName())
                    && !(action.getRequired()));
    assertThat(clientInterceptor.getActions())
        .anyMatch(
            action ->
                action.getKey().getName().equals(UserContext.CTX_KEY.getName())
                    && (action.getRequired()));
    assertThat(clientInterceptor.getActions())
        .anyMatch(
            action ->
                action.getKey().getName().equals(CatalogContext.CTX_KEY.getName())
                    && !(action.getRequired()));
  }
}
