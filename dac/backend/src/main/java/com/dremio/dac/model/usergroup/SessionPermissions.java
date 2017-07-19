/*
 * Copyright (C) 2017 Dremio Corporation
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

/**
 * Permissions associated with a particular user session.
 */
public class SessionPermissions {

  private final boolean canUploadProfiles;
  private final boolean canDownloadProfiles;
  private final boolean canEmailForSupport;
  private final boolean canChatForSupport;

  @JsonCreator
  public SessionPermissions(
      @JsonProperty("canUploadProfiles") boolean canUploadProfiles,
      @JsonProperty("canDownloadProfiles") boolean canDownloadProfiles,
      @JsonProperty("canEmailForSupport") boolean canEmailForSupport,
      @JsonProperty("canChatForSupport") boolean canChatForSupport) {
    super();
    this.canUploadProfiles = canUploadProfiles;
    this.canDownloadProfiles = canDownloadProfiles;
    this.canEmailForSupport = canEmailForSupport;
    this.canChatForSupport = canChatForSupport;
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


}
