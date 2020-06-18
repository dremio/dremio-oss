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
package com.dremio.dac.support;

import java.io.IOException;

import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.service.datasets.DatasetDownloadManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.Service;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.services.configuration.proto.ConfigurationEntry;

/**
 * Service responsible for generating cluster identity and upload data to Dremio
 * when a user asks for support help.
 */
@Options
public interface SupportService extends Service {
  int TIMEOUT_IN_SECONDS = 5 * 60;
  String DREMIO_LOG_PATH_PROPERTY = "dremio.log.path";

  TypeValidators.BooleanValidator USERS_CHAT = new TypeValidators.BooleanValidator("support.users.chat", false);
  TypeValidators.BooleanValidator USERS_UPLOAD = new TypeValidators.BooleanValidator("support.users.upload", true);
  TypeValidators.BooleanValidator USERS_DOWNLOAD = new TypeValidators.BooleanValidator("support.users.download", true);
  TypeValidators.BooleanValidator USERS_EMAIL = new TypeValidators.BooleanValidator("support.users.email", true);

  TypeValidators.StringValidator SUPPORT_EMAIL_ADDR = new TypeValidators.StringValidator("support.email.addr", "");
  TypeValidators.StringValidator SUPPORT_EMAIL_SUBJECT = new TypeValidators.StringValidator("support.email.jobs.subject", "");

  TypeValidators.StringValidator SUPPORT_UPLOAD_BASE = new TypeValidators.StringValidator("support.upload.base", "https://s3-us-west-2.amazonaws.com/supportuploads.dremio.com/");
  TypeValidators.StringValidator TEMPORARY_SUPPORT_PATH = new TypeValidators.StringValidator("support.temp", "/tmp/dremio-support/");
   TypeValidators.BooleanValidator OUTSIDE_COMMUNICATION_DISABLED =
   new TypeValidators.BooleanValidator("dremio.ui.outside_communication_disabled", false);

  String NAME = "support";

  String LOGS_STORAGE_PLUGIN = "__logs";
  String LOCAL_STORAGE_PLUGIN = "__support";
  String CLUSTER_ID = "clusterId";
  String CLUSTER_IDENTITY = "clusterIdentity";
  String DREMIO_EDITION = "dremioEdition";

  ClusterIdentity getClusterId();

  /**
   * Retrieve a config store entry
   *
   * @param key
   * @return config entry.
   */
  ConfigurationEntry getConfigurationEntry(String key);

  /**
   * Sets a config store entry
   *
   * @param key
   * @param entry
   */
  void setConfigurationEntry(String key, ConfigurationEntry entry);

  /**
   * Build a support zip file and upload it to s3.
   * @param userId
   * @param jobId
   * @return
   * @throws IOException
   * @throws UserNotFoundException
   */
  SupportResponse uploadSupportRequest(String userId, JobId jobId) throws IOException,
    UserNotFoundException;

  /**
   * Download a support request.
   * @param request
   * @return
   * @throws IOException
   * @throws UserNotFoundException
   * @throws com.dremio.service.jobs.JobNotFoundException
   */
  DatasetDownloadManager.DownloadDataResponse downloadSupportRequest(SupportRequest request)
    throws UserNotFoundException, IOException, JobNotFoundException;
}
