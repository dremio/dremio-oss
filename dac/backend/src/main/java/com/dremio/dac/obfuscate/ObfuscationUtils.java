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
package com.dremio.dac.obfuscate;

import static com.dremio.service.jobs.JobsProtoUtil.toBuf;
import static com.dremio.service.jobs.JobsProtoUtil.toStuff;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.utils.PathUtils;
import com.dremio.context.RequestContext;
import com.dremio.context.SupportContext;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.beans.QueryProfile;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat.ArrowFileMetadata;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.Reflection;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.google.common.annotations.VisibleForTesting;

/**
 * Utils functions related to obfuscation.
 * Each public util function should first check whether to obfuscate
 * and next step the actual obfuscation should be done.
 */
@Options
public class ObfuscationUtils {
  private static final String FULL_OBFUSCATION_PROPERTY_NAME = "dremio.supportconsole.fullobfuscation.enabled";
  private static boolean fullObfuscation = Boolean.getBoolean(FULL_OBFUSCATION_PROPERTY_NAME);
  private static Provider<OptionManager> optionManagerProvider = null;
  public static final TypeValidators.BooleanValidator PARTIAL_OBFUSCATION_ENABLED =
          new TypeValidators.BooleanValidator("dremio.supportconsole.obfuscation.partial.enabled", true);

  @VisibleForTesting
  public static void setFullObfuscation(boolean fullObfuscationParam){
    fullObfuscation = fullObfuscationParam;
  }

  public static void setOptionManagerProvider(Provider<OptionManager> optionManagerProviderParam) {
    optionManagerProvider = optionManagerProviderParam;
  }

  // return true only when support context is present in RequestContext
  public static boolean shouldObfuscatePartial() {
    return RequestContext.current().get(SupportContext.CTX_KEY) != null &&
            (optionManagerProvider == null ? true: optionManagerProvider.get() .getOption(PARTIAL_OBFUSCATION_ENABLED));
  }

  public static boolean shouldObfuscateFull() {
    return shouldObfuscatePartial() && fullObfuscation;
  }

  public static UserBitShared.QueryProfile obfuscate(UserBitShared.QueryProfile queryProfile) {
    if (!shouldObfuscatePartial()) {
      return queryProfile;
    }
    QueryProfile queryProfileStuff = toStuff(queryProfile);
    ProfileCleanser.cleanseQueryProfileInPlace(queryProfileStuff);
    return toBuf(queryProfileStuff);
  }

  public static JobProtobuf.ParentDatasetInfo obfuscate(JobProtobuf.ParentDatasetInfo parent) {
    if (!shouldObfuscateFull()) {
      return parent;
    }

    return JobProtobuf.ParentDatasetInfo.newBuilder()
      .mergeFrom(parent)
      .clearDatasetPath()
      .addAllDatasetPath(cleanseDatasetPath(parent.getDatasetPathList()))
      .build();
  }

  public static JobAttempt obfuscate(JobAttempt ja) {
    if (!shouldObfuscateFull()) {
      return ja;
    }
    return toStuff(obfuscate(toBuf(ja)));
  }

  public static JobProtobuf.JobAttempt obfuscate(JobProtobuf.JobAttempt ja) {
    if (!shouldObfuscatePartial()) {
      return ja;
    }

    JobProtobuf.JobAttempt.Builder jobAttemptBuilder = JobProtobuf.JobAttempt.newBuilder().mergeFrom(ja);
    if (ja.hasInfo()) {
      jobAttemptBuilder = jobAttemptBuilder.setInfo(obfuscate(ja.getInfo()));
    }

    if (ja.hasDetails()) {
      jobAttemptBuilder = jobAttemptBuilder.setDetails(obfuscate(ja.getDetails()));
    }
    return jobAttemptBuilder.build();
  }

  public static JobProtobuf.JobInfo obfuscate(JobProtobuf.JobInfo jobInfo) {
    if (!shouldObfuscatePartial()) {
      return jobInfo;
    }

    JobProtobuf.JobInfo.Builder jobInfoBuilder = JobProtobuf.JobInfo.newBuilder()
      .mergeFrom(jobInfo)
      .clearDatasetPath()
      .addAllDatasetPath(cleanseDatasetPath(jobInfo.getDatasetPathList()))
      .clearParents()
      .addAllParents(obfuscate(jobInfo.getParentsList(), ObfuscationUtils::obfuscate))
      .clearScanPaths()
      .addAllScanPaths(obfuscate(jobInfo.getScanPathsList(), ObfuscationUtils::obfuscate))
      .setSql(obfuscateSql(jobInfo.getSql()))
      .clearFieldOrigins()
      .addAllFieldOrigins(obfuscate(jobInfo.getFieldOriginsList(), ObfuscationUtils::obfuscate))
      .clearContext()
      .addAllContext(obfuscate(jobInfo.getContextList(), ObfuscationUtils::obfuscate))
      .clearQueriedDatasets()
      .addAllQueriedDatasets(obfuscate(jobInfo.getQueriedDatasetsList(), ObfuscationUtils::obfuscate))
      .clearReflectionsMatched()
      .addAllReflectionsMatched(obfuscate(jobInfo.getReflectionsMatchedList(), ObfuscationUtils::obfuscate))
      .clearResultMetadata()
      .addAllResultMetadata(obfuscate(jobInfo.getResultMetadataList(), ObfuscationUtils::obfuscate));

    // Check and obfuscate optional fields
    if (jobInfo.hasDescription()) {
      jobInfoBuilder = jobInfoBuilder.setDescription(obfuscateSql(jobInfo.getDescription()));
    }

    if (jobInfo.hasJoinAnalysis()) {
      jobInfoBuilder = jobInfoBuilder.clearJoinAnalysis().setJoinAnalysis(obfuscate(jobInfo.getJoinAnalysis()));
    }

    return jobInfoBuilder.build();
  }

  public static Reflection obfuscate(Reflection reflection) {
    if (!shouldObfuscateFull()) {
      return reflection;
    }
    return toStuff(obfuscate(toBuf(reflection)));
  }

  public static JobProtobuf.Reflection obfuscate(JobProtobuf.Reflection reflection) {
    if (!shouldObfuscateFull()) {
      return reflection;
    }

    JobProtobuf.Reflection.Builder builder = JobProtobuf.Reflection.newBuilder()
      .mergeFrom(reflection);

    if (reflection.hasDatasetName()) {
      builder = builder.setDatasetName(obfuscate(reflection.getDatasetName()));
    }

    if (reflection.hasReflectionName()) {
      builder = builder.setReflectionName(obfuscate(reflection.getReflectionName()));
    }

    if (reflection.hasReflectionDatasetPath()) {
      builder = builder.setReflectionDatasetPath(obfuscate(reflection.getReflectionDatasetPath()));
    }

    return builder.build();
  }

  public static DataSet obfuscate(DataSet dataset) {
    if (!shouldObfuscateFull()) {
      return dataset;
    }
    return toStuff(obfuscate(toBuf(dataset)));
  }

  public static JobProtobuf.DataSet obfuscate(JobProtobuf.DataSet dataset) {
    if (!shouldObfuscateFull()) {
      return dataset;
    }

    JobProtobuf.DataSet.Builder datasetBuilder = JobProtobuf.DataSet.newBuilder().mergeFrom(dataset)
      .clearReflectionsDefined()
      .addAllReflectionsDefined(obfuscate(dataset.getReflectionsDefinedList(), ObfuscationUtils::obfuscate));

    if (dataset.hasDatasetName()) {
      datasetBuilder = datasetBuilder.setDatasetName(obfuscate(dataset.getDatasetName()));
    }

    if (dataset.hasDatasetPath()) {
      datasetBuilder = datasetBuilder.setDatasetPath(obfuscate(dataset.getDatasetPath()));
      datasetBuilder = datasetBuilder.clearDatasetPaths();
      datasetBuilder = datasetBuilder.addAllDatasetPaths(obfuscate(dataset.getDatasetPathsList(), ObfuscationUtils::obfuscate));
    }

    return datasetBuilder.build();
  }

  private static ArrowFileMetadata obfuscate(ArrowFileMetadata arrowFileMetadata) {
    ArrowFileMetadata.Builder arrowFileMetadataBuilder = ArrowFileMetadata.newBuilder().mergeFrom(arrowFileMetadata);

    if (arrowFileMetadata.hasFooter()) {
      arrowFileMetadataBuilder = arrowFileMetadataBuilder.clearFooter().setFooter(obfuscate(arrowFileMetadata.getFooter()));
    }
    return arrowFileMetadataBuilder.build();
  }

  private static ArrowFileFormat.ArrowFileFooter obfuscate(ArrowFileFormat.ArrowFileFooter arrowFileFooter) {
    return ArrowFileFormat.ArrowFileFooter.newBuilder()
      .mergeFrom(arrowFileFooter)
      .clearField()
      .addAllField(obfuscate(arrowFileFooter.getFieldList(), ObfuscationUtils::obfuscate))
      .build();
  }

  private static UserBitShared.SerializedField obfuscate(UserBitShared.SerializedField serializedField) {
    UserBitShared.SerializedField.Builder serializedFieldBuilder = UserBitShared.SerializedField.newBuilder()
      .mergeFrom(serializedField)
      .clearChild()
      .addAllChild(obfuscate(serializedField.getChildList(), ObfuscationUtils::obfuscate));

    if (serializedField.hasNamePart()) {
      serializedFieldBuilder = serializedFieldBuilder.setNamePart(obfuscate(serializedField.getNamePart()));
    }

    return serializedFieldBuilder.build();
  }

  private static UserBitShared.NamePart obfuscate(UserBitShared.NamePart namePart) {
    UserBitShared.NamePart.Builder namePartBuilder = UserBitShared.NamePart.newBuilder().mergeFrom(namePart);
    if (namePart.hasName()) {
      namePartBuilder = namePartBuilder.setName(obfuscate(namePart.getName()));
    }
    if (namePart.hasChild()) {
      namePartBuilder = namePartBuilder.setChild(obfuscate(namePart.getChild()));
    }
    return namePartBuilder.build();
  }

  private static DatasetCommonProtobuf.FieldOrigin obfuscate(DatasetCommonProtobuf.FieldOrigin fo) {
    return DatasetCommonProtobuf.FieldOrigin.newBuilder()
      .mergeFrom(fo)
      .setName(obfuscate(fo.getName()))
      .clearOrigins()
      .addAllOrigins(obfuscate(fo.getOriginsList(), ObfuscationUtils::obfuscate))
      .build();
  }

  private static DatasetCommonProtobuf.Origin obfuscate(DatasetCommonProtobuf.Origin o) {
    return DatasetCommonProtobuf.Origin.newBuilder()
      .mergeFrom(o)
      .setColumnName(obfuscate(o.getColumnName()))
      .clearTable()
      .addAllTable(obfuscate(o.getTableList(), ObfuscationUtils::obfuscate))
      .build();
  }

  private static JobProtobuf.JoinAnalysis obfuscate(JobProtobuf.JoinAnalysis ja) {
    return JobProtobuf.JoinAnalysis.newBuilder()
      .mergeFrom(ja)
      .clearJoinTables()
      .addAllJoinTables(obfuscate(ja.getJoinTablesList(), ObfuscationUtils::obfuscate))
      .build();
  }

  private static JobProtobuf.JoinTable obfuscate(JobProtobuf.JoinTable jt) {
    return JobProtobuf.JoinTable.newBuilder()
      .mergeFrom(jt)
      .clearTableSchemaPath()
      .addAllTableSchemaPath(cleanseDatasetPath(jt.getTableSchemaPathList()))
      .build();
  }

  private static JobProtobuf.ScanPath obfuscate(JobProtobuf.ScanPath sp) {
    return JobProtobuf.ScanPath.newBuilder()
      .mergeFrom(sp)
      .clearPath()
      .addAllPath(cleanseDatasetPath(sp.getPathList()))
      .build();
  }

  public static JobProtobuf.TableDatasetProfile obfuscate(JobProtobuf.TableDatasetProfile tableDatasetProfile) {
    if (!shouldObfuscateFull()) {
      return tableDatasetProfile;
    }
    return JobProtobuf.TableDatasetProfile.newBuilder()
      .mergeFrom(tableDatasetProfile)
      .clearDatasetProfile()
      .setDatasetProfile(obfuscate(tableDatasetProfile.getDatasetProfile()))
      .build();
  }

  public static JobProtobuf.FileSystemDatasetProfile obfuscate(JobProtobuf.FileSystemDatasetProfile fileSystemDatasetProfile) {
    if (!shouldObfuscateFull()) {
      return fileSystemDatasetProfile;
    }
    return JobProtobuf.FileSystemDatasetProfile.newBuilder()
      .mergeFrom(fileSystemDatasetProfile)
      .clearDatasetProfile()
      .setDatasetProfile(obfuscate(fileSystemDatasetProfile.getDatasetProfile()))
      .build();
  }

  public static JobProtobuf.CommonDatasetProfile obfuscate(JobProtobuf.CommonDatasetProfile commonDatasetProfile) {
    if (!shouldObfuscateFull()) {
      return commonDatasetProfile;
    }

    return JobProtobuf.CommonDatasetProfile.newBuilder()
      .mergeFrom(commonDatasetProfile)
      .clearDatasetPaths()
      .addAllDatasetPaths(obfuscate(commonDatasetProfile.getDatasetPathsList(), ObfuscationUtils::obfuscate))
      .build();
  }

  public static JobProtobuf.DatasetPathUI obfuscate(JobProtobuf.DatasetPathUI datasetPathUI) {
    if (!shouldObfuscateFull()) {
      return datasetPathUI;
    }

    return JobProtobuf.DatasetPathUI.newBuilder()
      .mergeFrom(datasetPathUI)
      .clearDatasetPath()
      .addAllDatasetPath(cleanseDatasetPath(datasetPathUI.getDatasetPathList()))
      .build();
  }

  public static com.dremio.service.job.JobDetails obfuscate(com.dremio.service.job.JobDetails jobDetails) {
    if (!shouldObfuscatePartial()) {
      return jobDetails;
    }

    return com.dremio.service.job.JobDetails.newBuilder()
      .mergeFrom(jobDetails)
      .clearAttempts()
      .addAllAttempts(obfuscate(jobDetails.getAttemptsList(), ObfuscationUtils::obfuscate))
      .build();
  }

  public static JobDetails obfuscate(JobDetails jobDetails) {
    if (!shouldObfuscateFull()) {
      return jobDetails;
    }
    return toStuff(obfuscate(toBuf(jobDetails)));
  }

  public static JobProtobuf.JobDetails obfuscate(JobProtobuf.JobDetails jobDetailsBuf) {
    if (!shouldObfuscateFull()) {
      return jobDetailsBuf;
    }

    jobDetailsBuf = JobProtobuf.JobDetails.newBuilder()
      .mergeFrom(jobDetailsBuf)
      .clearFsDatasetProfiles()
      .addAllFsDatasetProfiles(obfuscate(jobDetailsBuf.getFsDatasetProfilesList(), ObfuscationUtils::obfuscate))
      .clearTableDatasetProfiles()
      .addAllTableDatasetProfiles(obfuscate(jobDetailsBuf.getTableDatasetProfilesList(), ObfuscationUtils::obfuscate))
      .build();

    return jobDetailsBuf;
  }

  public static JobSummary obfuscate(JobSummary jobSummary) {
    if (!shouldObfuscatePartial()) {
      return jobSummary;
    }

    String description = obfuscateSql(jobSummary.getDescription());
    String sql = obfuscateSql(jobSummary.getSql());
    List<String> datasetPath = cleanseDatasetPath(jobSummary.getDatasetPathList());
    JobSummary.Builder jobSummaryBuilder = JobSummary.newBuilder().mergeFrom(jobSummary)
      .setDescription(description)
      .setSql(sql)
      .clearParents()
      .addAllParents(obfuscate(jobSummary.getParentsList(), ObfuscationUtils::obfuscate))
      .addAllDatasetPath(datasetPath);

    if (jobSummary.hasParent()) {
      jobSummaryBuilder = jobSummaryBuilder.setParent(obfuscate(jobSummary.getParent()));
    }

    return jobSummaryBuilder.build();
  }

  public static String obfuscateSql(String sql) {
    if (!shouldObfuscatePartial()) {
      return sql;
    }
    return SqlCleanser.cleanseSql(sql);
  }

  public static <T> List<T> obfuscate(List<T> list, Function<T,T> itemFun) {
    if (!shouldObfuscatePartial()) {
      return list;
    }
    return list.stream().map(i->itemFun.apply(i)).collect(Collectors.toList());
  }

  public static List<String> cleanseDatasetPath(List<String> pathElements) {
    return obfuscate(pathElements, ObfuscationUtils::obfuscate);
  }

  public static String cleanseDatasetPath(String path) {
    if (!shouldObfuscateFull()) {
      return path;
    }
    List<String> pathElements = PathUtils.parseFullPath(path);
    List<String> redactedPathElements = obfuscate(pathElements, ObfuscationUtils::obfuscate);
    return PathUtils.constructFullPath(redactedPathElements);
  }

  public static String obfuscate(String str) {
    if (!shouldObfuscateFull()) {
      return str;
    }
    return Integer.toHexString(str.toLowerCase().hashCode());
  }

  /**
   * Obfuscate based on {@link #shouldObfuscatePartial}
   * @param str
   * @return
   */
  public static String obfuscatePartial(String str) {
    if (!shouldObfuscatePartial()) {
      return str;
    }
    return Integer.toHexString(str.toLowerCase().hashCode());
  }
}
