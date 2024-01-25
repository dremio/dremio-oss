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
package com.dremio.sabot.rpc.user;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.proto.UserBitShared.UserCredentials;
import com.dremio.exec.store.ischema.InfoSchemaConstants;
import com.google.common.collect.ImmutableList;

public class TestChangeTrackingUserSession {
  private static final String TEST_USER = "dummy@dummy.com";
  private static final String BRANCH_NAME = "branchName";
  private static final String SOURCE_NAME = "SourceName1";
  private static final String ENGINE = "preview";
  private static final UserCredentials USER_CREDENTIALS = UserCredentials.newBuilder().setUserName(TEST_USER).build();
  private static final ImmutableList<String> PATH_CONTEXT = ImmutableList.of("path1", "path2");
  private final String schemaName = "path1.path2";
  private static final VersionContext BRANCH = VersionContext.ofBranch(BRANCH_NAME);
  private static CaseInsensitiveMap<VersionContext> sourceVersionMap;
  private static UserSession inputUserSession;
  private static ChangeTrackingUserSession delegateCopiedFromUserSession;
  private static ChangeTrackingUserSession changeTrackingWithBuilderAttributes;
  private static ChangeTrackingUserSession delegateCopiedFromChangeTrackingUserSession;
  private final ChangeTrackingUserSession unmodifiedCTUS = ChangeTrackingUserSession.Builder.newBuilder().build();
  private final ChangeTrackingUserSession sameCTUS = ChangeTrackingUserSession.Builder.newBuilder(delegateCopiedFromUserSession).build();

  @BeforeClass
  public static void setup() {
    sourceVersionMap = CaseInsensitiveMap.newHashMap();
    sourceVersionMap.put(SOURCE_NAME, BRANCH);
    inputUserSession = UserSession.Builder.newBuilder()
      .withCredentials(USER_CREDENTIALS)
      .withDefaultSchema(PATH_CONTEXT)
      .withSourceVersionMapping(sourceVersionMap)
      .withErrorOnUnspecifiedVersion(true)
      .build();
    delegateCopiedFromUserSession = ChangeTrackingUserSession.Builder.newBuilder()
      .withDelegate(inputUserSession)
      .build();
    delegateCopiedFromChangeTrackingUserSession = ChangeTrackingUserSession.Builder.newBuilderWithCopy(delegateCopiedFromUserSession).build();
    changeTrackingWithBuilderAttributes = ChangeTrackingUserSession.Builder.newBuilder()
      .withDefaultSchema(PATH_CONTEXT)
      .withEngineName(ENGINE)
      .withErrorOnUnspecifiedVersion(true)
      .withSourceVersionMapping(sourceVersionMap)
      .build();
  }

  @Test
  public void testUnmodified() {
    Assert.assertNull(ENGINE, unmodifiedCTUS.getEngine());
    Assert.assertTrue(unmodifiedCTUS.getCredentials() == null);
    Assert.assertEquals(InfoSchemaConstants.IS_CATALOG_NAME, unmodifiedCTUS.getCatalogName());
    Assert.assertEquals("", unmodifiedCTUS.getDefaultSchemaName());
    Assert.assertEquals(unmodifiedCTUS.getSourceVersionMapping().size(), 0);
    Assert.assertFalse(unmodifiedCTUS.errorOnUnspecifiedVersion());
  }

  @Test
  public void testBuildWithDelegate() {
    Assert.assertEquals(TEST_USER, delegateCopiedFromUserSession.getCredentials().getUserName());
    Assert.assertEquals(InfoSchemaConstants.IS_CATALOG_NAME, delegateCopiedFromUserSession.getCatalogName());
    Assert.assertEquals(schemaName, delegateCopiedFromUserSession.getDefaultSchemaName());
    Assert.assertTrue(delegateCopiedFromUserSession.getSourceVersionMapping().get(SOURCE_NAME).equals(BRANCH));
    Assert.assertTrue(delegateCopiedFromUserSession.errorOnUnspecifiedVersion());
  }

  @Test
  public void testBuildWithDelegateAgainstInputUserSession() {
    Assert.assertEquals(inputUserSession.getCredentials().getUserName(), delegateCopiedFromUserSession.getCredentials().getUserName());
    Assert.assertEquals(inputUserSession.getCatalogName(), delegateCopiedFromUserSession.getCatalogName());
    Assert.assertEquals(inputUserSession.getDefaultSchemaName(), delegateCopiedFromUserSession.getDefaultSchemaName());
    Assert.assertEquals(inputUserSession.getSessionVersionForSource(SOURCE_NAME), delegateCopiedFromUserSession.getSessionVersionForSource(SOURCE_NAME));
    Assert.assertEquals(inputUserSession.errorOnUnspecifiedVersion(), delegateCopiedFromUserSession.errorOnUnspecifiedVersion());
  }

  @Test
  public void testSame() {
    Assert.assertEquals(delegateCopiedFromUserSession, sameCTUS);
  }

  @Test
  public void testDelegateCopiedFromChangeTrackingUserSession() {
    Assert.assertNotEquals(delegateCopiedFromUserSession, delegateCopiedFromChangeTrackingUserSession);
    Assert.assertEquals(delegateCopiedFromUserSession.getCredentials().getUserName(), delegateCopiedFromChangeTrackingUserSession.getCredentials().getUserName());
    Assert.assertEquals(delegateCopiedFromUserSession.getDefaultSchemaName(), delegateCopiedFromChangeTrackingUserSession.getDefaultSchemaName());
    Assert.assertEquals(delegateCopiedFromUserSession.getSessionVersionForSource(SOURCE_NAME), delegateCopiedFromChangeTrackingUserSession.getSessionVersionForSource(SOURCE_NAME));
    Assert.assertEquals(delegateCopiedFromUserSession.errorOnUnspecifiedVersion(), delegateCopiedFromChangeTrackingUserSession.errorOnUnspecifiedVersion());
  }

  @Test
  public void testChangeTrackingWithBuilderAttributes() {
    Assert.assertEquals(schemaName, changeTrackingWithBuilderAttributes.getDefaultSchemaName());
    Assert.assertEquals(ENGINE, changeTrackingWithBuilderAttributes.getEngine());
    Assert.assertTrue(changeTrackingWithBuilderAttributes.errorOnUnspecifiedVersion());
    Assert.assertTrue(changeTrackingWithBuilderAttributes.getSourceVersionMapping().get(SOURCE_NAME).equals(BRANCH));
  }
}
