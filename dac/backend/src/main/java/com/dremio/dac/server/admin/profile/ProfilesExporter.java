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
package com.dremio.dac.server.admin.profile;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.NotSupportedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.utils.ProtobufUtils;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.resource.ExportProfilesParams;
import com.dremio.dac.resource.ExportProfilesParams.ExportFormatType;
import com.dremio.dac.resource.ExportProfilesParams.WriteFileMode;
import com.dremio.dac.resource.ExportProfilesStats;
import com.dremio.dac.support.SupportService;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.dac.util.ZipUtil;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.AttemptId;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.jobs.AttemptIdUtils;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;

import io.protostuff.ProtostuffIOUtil;

/**
 * Export profiles utils
 */
public final class ProfilesExporter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfilesExporter.class);

  private String outputFilePath;
  private final WriteFileMode writeMode;
  private final Long fromDate;
  private final Long toDate;
  private final ExportFormatType outputFormatType;
  private final int chunkSize;

  public ProfilesExporter(ExportProfilesParams exportParams) {
    String path = exportParams.getOutputFilePath();
    if(path.charAt(path.length()-1) != '/'){ // in case '/' is missing at the end
      path = path + '/';
    }
    this.outputFilePath = path;
    this.writeMode = exportParams.getWriteMode();
    this.fromDate = exportParams.getFromDate();
    this.toDate = exportParams.getToDate();
    this.outputFormatType = exportParams.getOutputFormatType();
    this.chunkSize = exportParams.getChunkSize();
  }

  private boolean isLocalFileSystem(String uriSchema) {
    return uriSchema == null || "file".equals(uriSchema);
  }

  private Path getProfileFileNameWithPath(String profileId) {
    String full_path = outputFilePath + "profile_" + profileId + ".json";
    return new Path(full_path);
  }

  private IndexedStore.FindByCondition getJobsFilter(Long fromDate, Long toDate) {
    IndexedStore.FindByCondition jobsFilter = new IndexedStore.FindByCondition();

    // date condition
    SearchQuery dateQuery = SearchQueryUtils.newRangeLong(JobIndexKeys.END_TIME.getIndexFieldName(),
      fromDate, toDate, true, false);

    // job state condition
    String jobStatusKey = JobIndexKeys.JOB_STATE.getIndexFieldName();
    SearchQuery jobStateQuery = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery(jobStatusKey, JobState.FAILED.toString()),
      SearchQueryUtils.newTermQuery(jobStatusKey, JobState.CANCELED.toString()),
      SearchQueryUtils.newTermQuery(jobStatusKey, JobState.COMPLETED.toString())
    );

    jobsFilter.setCondition(SearchQueryUtils.and(dateQuery, jobStateQuery)).setPageSize(100);

    return jobsFilter;
  }

  /**
   * Checks if files should be skipped
   * @param fs file system abstraction
   * @param fileName file name to check
   * @return returns true if file already exists and export options are set to skip such files.
   * @throws IOException
   */
  private boolean skipFile(FileSystem fs, Path fileName)
    throws IOException {
    if (fs.exists(fileName)) {
      switch (writeMode) {
        case FAIL_IF_EXISTS:
          throw new IOException(String.format("File '%s' already exists", fileName));
        case SKIP:
          return true;
        case OVERWRITE:
          return false;
        default:
          throw new NotSupportedException(String.format("Not supported write mode: %d", writeMode));
      }
    }

    return false;
  }

  public final ExportProfilesStats export(KVStoreProvider provider)
    throws Exception {

    Path fakeFileName = getProfileFileNameWithPath("fake_id");
    Configuration conf = new Configuration();
    FileSystem fs = fakeFileName.getFileSystem(conf);
    boolean isLocalFileSystem = isLocalFileSystem(fs.getScheme());
    fs.setWriteChecksum(!isLocalFileSystem);
    fs.setVerifyChecksum(!isLocalFileSystem);

    BackupRestoreUtil.checkOrCreateDirectory(fs, fakeFileName.getParent());

    //Append ClusterId to outputPath
    Optional<ClusterIdentity> clusterIdentity = getClusterIdentity(new ConfigurationStore(provider),provider);
    if (!clusterIdentity.isPresent()) {
      throw new Exception("Cluster ID doesn't exist");
    }
    outputFilePath = outputFilePath + clusterIdentity.get().getIdentity() + "/";

    if (outputFormatType == ExportFormatType.JSON) {
      return exportJSON(fs, provider);
    }
    return exportChunk(fs, provider);
  }

  private ExportProfilesStats exportJSON(FileSystem fs, KVStoreProvider provider)
    throws IOException {
    final KVStore<AttemptId, UserBitShared.QueryProfile> profilesStore = provider.getStore(LocalJobsService.JobsProfileCreator.class);
    final IndexedStore<JobId, JobResult> jobsStore = provider.getStore(LocalJobsService.JobsStoreCreator.class);

    IndexedStore.FindByCondition jobsFilter = getJobsFilter(fromDate, toDate);

    Integer totalJobsCount = jobsStore.getCounts(jobsFilter.getCondition()).get(0);
    logger.debug("Job count: {}", totalJobsCount);
    long profilesCount = 0;
    long skippedProfilesCount = 0;

    logger.debug("Profiles export is started");
    for(Map.Entry<JobId, JobResult> jobEntry: jobsStore.find(jobsFilter)) {
      for (JobAttempt attempt : jobEntry.getValue().getAttemptsList()) {
        if (attempt.getAttemptId() == null) {
          logger.debug("failed to get an id for attempt: {}", attempt);
          continue;
        }
        UserBitShared.QueryProfile profile = profilesStore.get(AttemptIdUtils.fromString(attempt.getAttemptId()));
        if (profile == null) {
          logger.debug("Profile for attempt id: '{}' was not found", attempt.getAttemptId());
          continue;
        }
        profilesCount++;

        Path fileName = getProfileFileNameWithPath(attempt.getAttemptId());

        if (skipFile(fs, fileName)) {
          logger.debug("'{}' attempt is skipped as file already exists", attempt.getAttemptId());
          skippedProfilesCount++;
          //do not use continue here to log totalCount in the end of the loop
        } else {
          try (
            final OutputStream fsout = fs.create(fileName, true);
            final BufferedOutputStream bufferedOut = new BufferedOutputStream(fsout);
          ) {
            ProtobufUtils.writeAsJSONTo(fsout, profile);
          }
        }

        if (profilesCount % 1000 == 0) { // todo would not work if file skipped.
          logger.debug("{} profiles are processed", profilesCount);
        }
      }
    }
    logger.debug("Export is completed. {} profiles are processed.", profilesCount);

    return new ExportProfilesStats(totalJobsCount, profilesCount, skippedProfilesCount, outputFilePath);
  }

  private ExportProfilesStats exportChunk(FileSystem fs, KVStoreProvider provider)
    throws IOException {
    final KVStore<AttemptId, UserBitShared.QueryProfile> profilesStore = provider.getStore(LocalJobsService.JobsProfileCreator.class);
    final IndexedStore<JobId, JobResult> jobsStore = provider.getStore(LocalJobsService.JobsStoreCreator.class);

    IndexedStore.FindByCondition jobsFilter = getJobsFilter(fromDate, toDate);

    Integer totalJobsCount = jobsStore.getCounts(jobsFilter.getCondition()).get(0);
    logger.debug("Job count: {}", totalJobsCount);
    long profilesCount = 0;
    long skippedProfilesCount = 0;
    int capacity = chunkSize * 1_000_000;

    ZipUtil chunkWriter = new ZipUtil(capacity, fs, outputFilePath);

    logger.debug("Profiles export is started");
    for(Map.Entry<JobId, JobResult> jobEntry: jobsStore.find(jobsFilter)) {
      for (JobAttempt attempt : jobEntry.getValue().getAttemptsList()) {
        if (attempt.getAttemptId() == null) {
          logger.debug("failed to get an id for attempt: {}", attempt);
          continue;
        }
        UserBitShared.QueryProfile profile = profilesStore.get(AttemptIdUtils.fromString(attempt.getAttemptId()));
        if (profile == null) {
          logger.debug("Profile for attempt id: '{}' was not found", attempt.getAttemptId());
          continue;
        }

        chunkWriter.writeFile(String.format("profile_%s.JSON",attempt.getAttemptId()), ProtobufUtils.toJSONByteArray(profile));
        profilesCount++;

        if (profilesCount % 1000 == 0) {
          logger.debug("{} profiles are processed", profilesCount);
        }
      }
    }

    chunkWriter.close();

    logger.debug("Export is completed. {} profiles are processed.", profilesCount);

    return new ExportProfilesStats(totalJobsCount, profilesCount, skippedProfilesCount, outputFilePath);
  }

  private Optional<ClusterIdentity> getClusterIdentity(ConfigurationStore store, KVStoreProvider provider) {
    final ConfigurationEntry entry = store.get(SupportService.CLUSTER_ID);
    try {
      ClusterIdentity identity = ClusterIdentity.getSchema().newMessage();
      ProtostuffIOUtil.mergeFrom(entry.getValue().toByteArray(), identity, ClusterIdentity.getSchema());
      return Optional.ofNullable(identity);
    } catch (Exception e) {
      logger.info("failed to get cluster identity", e);
      return Optional.empty();
    }
  }
}
