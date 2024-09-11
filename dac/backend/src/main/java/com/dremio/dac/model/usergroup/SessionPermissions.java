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
package com.dremio.dac.model.usergroup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Permissions associated with a particular user session. */
public class SessionPermissions {

  private final boolean canUploadProfiles;
  private final boolean canDownloadProfiles;
  private final boolean canEmailForSupport;
  private final boolean canChatForSupport;
  private final boolean canViewAllJobs;
  private final boolean canCreateUser;
  private final boolean canCreateRole;
  private final boolean canCreateSource;
  private final boolean canUploadFile;
  private final boolean canManageNodeActivity;
  private final boolean canManageEngines;
  private final boolean canManageQueues;
  private final boolean canManageEngineRouting;
  private final boolean canManageSupportSettings;
  private final boolean canConfigureSecurity;
  private final boolean canRunDiagnostic;

  @JsonCreator
  public SessionPermissions(
      @JsonProperty("canUploadProfiles") boolean canUploadProfiles,
      @JsonProperty("canDownloadProfiles") boolean canDownloadProfiles,
      @JsonProperty("canEmailForSupport") boolean canEmailForSupport,
      @JsonProperty("canChatForSupport") boolean canChatForSupport,
      @JsonProperty("canViewAllJobs") boolean canViewAllJobs,
      @JsonProperty("canCreateUser") boolean canCreateUser,
      @JsonProperty("canCreateRole") boolean canCreateRole,
      @JsonProperty("canCreateSource") boolean canCreateSource,
      @JsonProperty("canUploadFile") boolean canUploadFile,
      @JsonProperty("canManageNodeActivity") boolean canManageNodeActivity,
      @JsonProperty("canManageEngines") boolean canManageEngines,
      @JsonProperty("canManageQueues") boolean canManageQueues,
      @JsonProperty("canManageEngineRouting") boolean canManageEngineRouting,
      @JsonProperty("canManageSupportSettings") boolean canManageSupportSettings,
      @JsonProperty("canConfigureSecurity") boolean canConfigureSecurity,
      @JsonProperty("canRunDiagnostic") boolean canRunDiagnostic) {
    super();
    this.canUploadProfiles = canUploadProfiles;
    this.canDownloadProfiles = canDownloadProfiles;
    this.canEmailForSupport = canEmailForSupport;
    this.canChatForSupport = canChatForSupport;
    this.canViewAllJobs = canViewAllJobs;
    this.canCreateUser = canCreateUser;
    this.canCreateRole = canCreateRole;
    this.canCreateSource = canCreateSource;
    this.canUploadFile = canUploadFile;
    this.canManageNodeActivity = canManageNodeActivity;
    this.canManageEngines = canManageEngines;
    this.canManageQueues = canManageQueues;
    this.canManageEngineRouting = canManageEngineRouting;
    this.canManageSupportSettings = canManageSupportSettings;
    this.canConfigureSecurity = canConfigureSecurity;
    this.canRunDiagnostic = canRunDiagnostic;
  }

  public boolean isCanUploadProfiles() {
    return canUploadProfiles;
  }

  public boolean isCanDownloadProfiles() {
    return canDownloadProfiles;
  }

  public boolean isCanEmailForSupport() {
    return canEmailForSupport;
  }

  public boolean isCanChatForSupport() {
    return canChatForSupport;
  }

  public boolean isCanViewAllJobs() {
    return canViewAllJobs;
  }

  public boolean isCanCreateUser() {
    return canCreateUser;
  }

  public boolean isCanCreateRole() {
    return canCreateRole;
  }

  public boolean isCanCreateSource() {
    return canCreateSource;
  }

  public boolean isCanUploadFile() {
    return canUploadFile;
  }

  public boolean isCanManageNodeActivity() {
    return canManageNodeActivity;
  }

  public boolean isCanManageEngines() {
    return canManageEngines;
  }

  public boolean isCanManageQueues() {
    return canManageQueues;
  }

  public boolean isCanManageEngineRouting() {
    return canManageEngineRouting;
  }

  public boolean isCanManageSupportSettings() {
    return canManageSupportSettings;
  }

  public boolean isCanConfigureSecurity() {
    return canConfigureSecurity;
  }

  public boolean isCanRunDiagnostic() {
    return canRunDiagnostic;
  }
}
