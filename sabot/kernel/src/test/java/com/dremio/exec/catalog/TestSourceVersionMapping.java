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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.users.SystemUser;

public class TestSourceVersionMapping {

  @Test
  public void TestRequestOptionsGet() {
    //Setup

    final String branchName = "branchName";
    final VersionContext branch = VersionContext.ofBranch(branchName);
    CaseInsensitiveMap<VersionContext> sourceVersionMap1 = CaseInsensitiveMap.newHashMap();
    sourceVersionMap1.put("SourceName1", branch);
    final MetadataRequestOptions requestOptions = MetadataRequestOptions.newBuilder()
      .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
      .setSourceVersionMapping(sourceVersionMap1)
      .build();

    //Act and Assert

    //Try different casings of the key to check that it matches the entry in the SourceVersionMapping
    assertTrue(requestOptions.getSourceVersionMapping().containsKey("sourcename1"));
    assertTrue(requestOptions.getSourceVersionMapping().containsKey("SourceName1"));
    assertTrue(requestOptions.getSourceVersionMapping().containsKey("SoUrceNAme1"));

    //Try different casings of key input to retrieve value.
    assertEquals(requestOptions.getSourceVersionMapping().get("sourceName1"), branch);
    assertEquals(requestOptions.getSourceVersionMapping().get("sourceNAME1"), branch);
    assertEquals(requestOptions.getSourceVersionMapping().get("SourCeNaMe1"), branch);
  }

  @Test
  public void TestRequestOptionsCloneWith() {
    //Setup

    final String branchName = "branchName";
    final VersionContext branch = VersionContext.ofBranch(branchName);
    CaseInsensitiveMap<VersionContext> sourceVersionMap1 = CaseInsensitiveMap.newHashMap();
    sourceVersionMap1.put("SourceName1", branch);
    final MetadataRequestOptions requestOptions = MetadataRequestOptions.newBuilder()
      .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
      .setSourceVersionMapping(sourceVersionMap1)
      .build();

    //Act

    //Create a new input source with different casing from existing entry
    Map<String, VersionContext> sourceVersionMap2 = new HashMap<>();
    //Add another entry with the key matching existing key bit different casing
    sourceVersionMap2.put("SourceNAMe1", branch);
    //Add a new entry with a different key
    sourceVersionMap2.put("SourceNAME2", branch);
    MetadataRequestOptions clonedRequestOptions = requestOptions.cloneWith(sourceVersionMap2);

    //Assert

    // Ensure that the cloned copy did not create a new entry in SourceVersionMapping
    // even though the casing of the second input is different
    assertTrue(clonedRequestOptions.getSourceVersionMapping().size() == 2);
    assertTrue(clonedRequestOptions.getSourceVersionMapping().containsKey("sourcename1"));
    assertTrue(clonedRequestOptions.getSourceVersionMapping().containsKey("sourcename2"));

  }

  @Test
  public void TestUserSessionSet() {
    //Setup
    final UserSession userSession = UserSession.Builder.newBuilder().build();
    final String branchName1 = "branchName1";
    final String branchName2 = "branchName1";
    final VersionContext branch1 = VersionContext.ofBranch(branchName1);
    final VersionContext branch2 = VersionContext.ofBranch(branchName2);
    String sourceName = "SourceName1";
    String sourceNameDifferentCase = "sourceNAme1";

    //Act
    userSession.setSessionVersionForSource(sourceName, branch1);
    userSession.setSessionVersionForSource(sourceNameDifferentCase, branch2);

    //Assert
    // Check that no new entry got added and the entry matches the original one
    assertTrue(userSession.getSourceVersionMapping().size() == 1);
    assertTrue(userSession.getSourceVersionMapping().containsKey("sourcename1"));
  }

  @Test
  public void TestUserSessionGet() {
    //Setup
    final String branchName1 = "branchName1";
    final VersionContext branch1 = VersionContext.ofBranch(branchName1);
    String sourceName = "sourceName1";
    Map<String, VersionContext> sourceVersion = new HashMap<>();
    sourceVersion.put(sourceName, branch1);
    final UserSession userSession = UserSession.Builder.newBuilder()
      .withSourceVersionMapping(sourceVersion)
      .build();

    //Act and Assert
    assertTrue(userSession.getSourceVersionMapping().containsKey("sourcename1"));
    assertTrue(userSession.getSourceVersionMapping().containsKey("SourceName1"));
    assertTrue(userSession.getSourceVersionMapping().containsKey("SoUrceNAme1"));

    //Try different casings of key input to retrieve value.
    assertEquals(userSession.getSourceVersionMapping().get("sourceName1"), branch1);
    assertEquals(userSession.getSourceVersionMapping().get("sourceNAME1"), branch1);
    assertEquals(userSession.getSourceVersionMapping().get("SourCeNaMe1"), branch1);
  }

}
