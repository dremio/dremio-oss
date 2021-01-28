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
package com.dremio.dac.server.models;


/**
 * A POJO which represents a server configuration that is injected in a html page and used by UI code
 *
 * <b>IMPORTANT!!!</b> if you change any field name you must change UI code respectively
 */
public class ServerData {
  private final String serverEnvironment;
  private final String serverStatus;
  private final String intercomAppId;
  private final boolean shouldEnableBugFiling;
  private final boolean shouldEnableRSOD;
  private final String supportEmailTo;
  private final String supportEmailSubjectForJobs;
  private final boolean outsideCommunicationDisabled;
  private final boolean subhourAccelerationPoliciesEnabled;
  private final boolean lowerProvisioningSettingsEnabled;
  private final boolean allowFileUploads;
  private final boolean allowSpaceManagement;
  private final String tdsMimeType;
  private final String whiteLabelUrl;
  private final String clusterId;
  private final String edition;
  private final AnalyzeTools analyzeTools;
  private final boolean crossSourceDisabled;
  private final boolean queryBundleUsersEnabled;
  private final long downloadRecordsLimit;

  protected ServerData(Builder builder) {
    this.serverEnvironment = builder.serverEnvironment;
    this.serverStatus = builder.serverStatus;
    this.intercomAppId = builder.intercomAppId;
    this.shouldEnableBugFiling = builder.shouldEnableBugFiling;
    this.shouldEnableRSOD = builder.shouldEnableRSOD;
    this.supportEmailTo = builder.supportEmailTo;
    this.supportEmailSubjectForJobs = builder.supportEmailSubjectForJobs;
    this.outsideCommunicationDisabled = builder.outsideCommunicationDisabled;
    this.subhourAccelerationPoliciesEnabled = builder.subhourAccelerationPoliciesEnabled;
    this.lowerProvisioningSettingsEnabled = builder.lowerProvisioningSettingsEnabled;
    this.allowFileUploads = builder.allowFileUploads;
    this.allowSpaceManagement = builder.allowSpaceManagement;
    this.tdsMimeType = builder.tdsMimeType;
    this.whiteLabelUrl = builder.whiteLabelUrl;
    this.clusterId = builder.clusterId;
    this.edition = builder.edition;
    this.analyzeTools = builder.analyzeTools;
    this.crossSourceDisabled = builder.crossSourceDisabled;
    this.queryBundleUsersEnabled = builder.queryBundleUsersEnabled;
    this.downloadRecordsLimit = builder.downloadRecordsLimit;
  }

  public String getServerEnvironment() {
    return serverEnvironment;
  }

  public String getServerStatus() {
    return serverStatus;
  }

  public String getIntercomAppId() {
    return intercomAppId;
  }

  public boolean isShouldEnableBugFiling() {
    return shouldEnableBugFiling;
  }

  public boolean isShouldEnableRSOD() {
    return shouldEnableRSOD;
  }

  public String getSupportEmailTo() {
    return supportEmailTo;
  }

  public String getSupportEmailSubjectForJobs() {
    return supportEmailSubjectForJobs;
  }

  public boolean isOutsideCommunicationDisabled() {
    return outsideCommunicationDisabled;
  }

  public boolean isSubhourAccelerationPoliciesEnabled() {
    return subhourAccelerationPoliciesEnabled;
  }

  public boolean isLowerProvisioningSettingsEnabled() {
    return lowerProvisioningSettingsEnabled;
  }

  public boolean isAllowFileUploads() {
    return allowFileUploads;
  }

  public boolean isAllowSpaceManagement() {
    return allowSpaceManagement;
  }

  public String getTdsMimeType() {
    return tdsMimeType;
  }

  public String getWhiteLabelUrl() {
    return whiteLabelUrl;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getEdition() {
    return edition;
  }

  public AnalyzeTools getAnalyzeTools() {
    return analyzeTools;
  }

  public boolean getQueryBundleUsersEnabled() {
    return queryBundleUsersEnabled;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(Builder builder) {
    return new Builder(builder);
  }

  public boolean isCrossSourceDisabled() {
    return crossSourceDisabled;
  }

  public long getDownloadRecordsLimit() {
    return downloadRecordsLimit;
  }

  /**
   * A builder for server data
   */
  public static class Builder {
    private String serverEnvironment;
    private String serverStatus;
    private String intercomAppId;
    private boolean shouldEnableBugFiling;
    private boolean shouldEnableRSOD;
    private String supportEmailTo;
    private String supportEmailSubjectForJobs;
    private boolean outsideCommunicationDisabled;
    private boolean subhourAccelerationPoliciesEnabled;
    private boolean lowerProvisioningSettingsEnabled;
    private boolean allowFileUploads;
    private boolean allowSpaceManagement;
    private String tdsMimeType;
    private String whiteLabelUrl;
    private String clusterId;
    private String edition;
    private AnalyzeTools analyzeTools;
    private boolean crossSourceDisabled;
    private boolean queryBundleUsersEnabled;
    private long downloadRecordsLimit;

    protected Builder() {
    }

    protected Builder(Builder builder) {
      this.serverEnvironment = builder.serverEnvironment;
      this.serverStatus = builder.serverStatus;
      this.intercomAppId = builder.intercomAppId;
      this.shouldEnableBugFiling = builder.shouldEnableBugFiling;
      this.shouldEnableRSOD = builder.shouldEnableRSOD;
      this.supportEmailTo = builder.supportEmailTo;
      this.supportEmailSubjectForJobs = builder.supportEmailSubjectForJobs;
      this.outsideCommunicationDisabled = builder.outsideCommunicationDisabled;
      this.subhourAccelerationPoliciesEnabled = builder.subhourAccelerationPoliciesEnabled;
      this.lowerProvisioningSettingsEnabled = builder.lowerProvisioningSettingsEnabled;
      this.allowFileUploads = builder.allowFileUploads;
      this.allowSpaceManagement = builder.allowSpaceManagement;
      this.tdsMimeType = builder.tdsMimeType;
      this.whiteLabelUrl = builder.whiteLabelUrl;
      this.clusterId = builder.clusterId;
      this.edition = builder.edition;
      this.analyzeTools = builder.analyzeTools;
      this.crossSourceDisabled = builder.crossSourceDisabled;
      this.queryBundleUsersEnabled = builder.queryBundleUsersEnabled;
      this.downloadRecordsLimit = builder.downloadRecordsLimit;
    }

    public Builder setServerEnvironment(String serverEnvironment) {
      this.serverEnvironment = serverEnvironment;
      return this;
    }

    public Builder setServerStatus(String serverStatus) {
      this.serverStatus = serverStatus;
      return this;
    }

    public Builder setIntercomAppId(String intercomAppId) {
      this.intercomAppId = intercomAppId;
      return this;
    }

    public Builder setShouldEnableBugFiling(boolean shouldEnableBugFiling) {
      this.shouldEnableBugFiling = shouldEnableBugFiling;
      return this;
    }

    public Builder setShouldEnableRSOD(boolean shouldEnableRSOD) {
      this.shouldEnableRSOD = shouldEnableRSOD;
      return this;
    }

    public Builder setSupportEmailTo(String supportEmailTo) {
      this.supportEmailTo = supportEmailTo;
      return this;
    }

    public Builder setSupportEmailSubjectForJobs(String supportEmailSubjectForJobs) {
      this.supportEmailSubjectForJobs = supportEmailSubjectForJobs;
      return this;
    }

    public Builder setOutsideCommunicationDisabled(boolean outsideCommunicationDisabled) {
      this.outsideCommunicationDisabled = outsideCommunicationDisabled;
      return this;
    }

    public Builder setSubhourAccelerationPoliciesEnabled(boolean subhourAccelerationPoliciesEnabled) {
      this.subhourAccelerationPoliciesEnabled = subhourAccelerationPoliciesEnabled;
      return this;
    }

    public Builder setLowerProvisioningSettingsEnabled(boolean lowerProvisioningSettingsEnabled) {
      this.lowerProvisioningSettingsEnabled = lowerProvisioningSettingsEnabled;
      return this;
    }

    public Builder setAllowFileUploads(boolean allowFileUploads) {
      this.allowFileUploads = allowFileUploads;
      return this;
    }

    public Builder setAllowSpaceManagement(boolean allowSpaceManagement) {
      this.allowSpaceManagement = allowSpaceManagement;
      return this;
    }

    public Builder setTdsMimeType(String tdsMimeType) {
      this.tdsMimeType = tdsMimeType;
      return this;
    }

    public Builder setWhiteLabelUrl(String whiteLabelUrl) {
      this.whiteLabelUrl = whiteLabelUrl;
      return this;
    }

    public Builder setClusterId(String clusterId) {
      this.clusterId = clusterId;
      return this;
    }

    public Builder setEdition(String edition) {
      this.edition = edition;
      return this;
    }

    public Builder setAnalyzeTools(AnalyzeTools analyzeTools) {
      this.analyzeTools = analyzeTools;
      return this;
    }

    public Builder setCrossSourceDisabled(boolean crossSourceDisabled) {
      this.crossSourceDisabled = crossSourceDisabled;
      return this;
    }

    public Builder setQueryBundleUsersEnabled(boolean queryBundleUsersEnabled) {
      this.queryBundleUsersEnabled = queryBundleUsersEnabled;
      return this;
    }

    public Builder setDownloadRecordsLimit(final long downloadRecordsLimit) {
      this.downloadRecordsLimit = downloadRecordsLimit;
      return this;
    }

    public ServerData build() {
      return new ServerData(this);
    }
  }
}


