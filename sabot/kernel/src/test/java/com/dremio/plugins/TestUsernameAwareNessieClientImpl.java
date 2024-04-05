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
package com.dremio.plugins;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.VersionContext;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.context.UsernameContext;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class TestUsernameAwareNessieClientImpl {
  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private NessieClient nessieClient;

  @Mock private UserService userService;

  @Mock private User user;

  @InjectMocks private UsernameAwareNessieClientImpl usernameAwareNessieClient;

  private static final String USER_NAME = "testUser";

  @Test
  public void createNamespace() throws Exception {
    List<String> namespaceList = Collections.singletonList("folder1");
    VersionContext versionContext = VersionContext.ofBranch("main");
    String userId = UUID.randomUUID().toString();

    when(userService.getUser(new UID(userId))).thenReturn(user);
    when(user.getUserName()).thenReturn(USER_NAME);
    doAnswer(
            (unused) -> {
              assertThat(RequestContext.current().get(UsernameContext.CTX_KEY).getUserName())
                  .isEqualTo(USER_NAME);
              return null;
            })
        .when(nessieClient)
        .createNamespace(namespaceList, versionContext);

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext(userId))
        .run(() -> usernameAwareNessieClient.createNamespace(namespaceList, versionContext));
  }

  @Test
  public void getDefaultBranch() throws Exception {
    String userId = UUID.randomUUID().toString();

    when(userService.getUser(new UID(userId))).thenReturn(user);
    when(user.getUserName()).thenReturn(USER_NAME);
    doAnswer(
            (unused) -> {
              assertThat(RequestContext.current().get(UsernameContext.CTX_KEY).getUserName())
                  .isEqualTo(USER_NAME);
              return null;
            })
        .when(nessieClient)
        .getDefaultBranch();

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext(userId))
        .call(usernameAwareNessieClient::getDefaultBranch);
  }

  @Test
  public void deleteNamespace() throws Exception {
    List<String> namespaceList = Collections.singletonList("folder1");
    VersionContext versionContext = VersionContext.ofBranch("main");
    String userId = UUID.randomUUID().toString();

    when(userService.getUser(new UID(userId))).thenReturn(user);
    when(user.getUserName()).thenReturn(USER_NAME);
    doAnswer(
            (unused) -> {
              assertThat(RequestContext.current().get(UsernameContext.CTX_KEY).getUserName())
                  .isEqualTo(USER_NAME);
              return null;
            })
        .when(nessieClient)
        .deleteNamespace(namespaceList, versionContext);

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext(userId))
        .run(() -> usernameAwareNessieClient.deleteNamespace(namespaceList, versionContext));
  }

  @Test
  public void createNamespaceWhenUserNotFoundException() throws Exception {
    List<String> namespaceList = Collections.singletonList("folder1");
    VersionContext versionContext = VersionContext.ofBranch("main");
    String userId = UUID.randomUUID().toString();

    when(userService.getUser(new UID(userId))).thenThrow(UserNotFoundException.class);
    doAnswer(
            (unused) -> {
              assertThat(RequestContext.current().get(UsernameContext.CTX_KEY)).isNull();
              return null;
            })
        .when(nessieClient)
        .createNamespace(namespaceList, versionContext);

    RequestContext.current()
        .with(UserContext.CTX_KEY, new UserContext(userId))
        .run(() -> usernameAwareNessieClient.createNamespace(namespaceList, versionContext));
  }
}
