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
package com.dremio.exec.store.sys.accel;

import java.sql.Timestamp;
import java.util.Iterator;

import com.dremio.service.Service;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC;
import com.google.common.base.Optional;

/**
 * Exposes the acceleration store to the execution engine
 */
public interface AccelerationListManager extends Service {

  Iterator<ReflectionInfo> getReflections();
  Iterator<MaterializationInfo> getMaterializations();
  Iterator<RefreshInfo> getRefreshInfos();
  Iterator<DependencyInfo> getReflectionDependencies();

  class ReflectionInfo {
    public final String reflection_id;
    public final String name;
    public final String type;
    public final String status;
    public final int num_failures;
    public final String datasetId;
    public final String dataset;
    public final String datasetType;
    public final String sortColumns;
    public final String partitionColumns;
    public final String distributionColumns;
    public final String dimensions;
    public final String measures;
    public final String displayColumns;
    public final String externalReflection;
    public final boolean arrow_cache;

    public ReflectionInfo(String reflectionId, String reflectionName, String type, String status, int numFailures, String datasetId,
        String datasetName, String datasetType, String sortColumns, String partitionColumns, String distributionColumns,
        String dimensions, String measures, String displayColumns, String externalReflection, boolean arrowCachingEnabled) {
      this.reflection_id = reflectionId;
      this.name = reflectionName;
      this.type = type;
      this.status = status;
      this.num_failures = numFailures;
      this.datasetId = datasetId;
      this.dataset = datasetName;
      this.datasetType = datasetType;
      this.sortColumns = sortColumns;
      this.partitionColumns = partitionColumns;
      this.distributionColumns = distributionColumns;
      this.dimensions = dimensions;
      this.measures = measures;
      this.displayColumns = displayColumns;
      this.externalReflection = externalReflection;
      this.arrow_cache = arrowCachingEnabled;
    }

    public ReflectionDescriptionServiceRPC.ListReflectionsResponse toProto() {
      ReflectionDescriptionServiceRPC.ListReflectionsResponse.Builder protoReflectionInfo =
        ReflectionDescriptionServiceRPC.ListReflectionsResponse.newBuilder();

        if(externalReflection != null) {
          protoReflectionInfo.setExternalReflection(externalReflection);
        }
        if (reflection_id != null) {
          protoReflectionInfo.setReflectionId(reflection_id);
        }
        if(datasetId != null) {
          protoReflectionInfo.setDatasetId(datasetId);
        }
        if(dataset != null) {
          protoReflectionInfo.setDatasetName(dataset);
        }
        if(datasetType != null) {
          protoReflectionInfo.setDatasetType(datasetType);
        }
        if(dimensions != null) {
          protoReflectionInfo.setDimensions(dimensions);
        }
        if (displayColumns != null) {
          protoReflectionInfo.setDisplayColumns(displayColumns);
        }
        if (distributionColumns != null) {
          protoReflectionInfo.setDistributionColumns(distributionColumns);
        }
        if (name != null) {
          protoReflectionInfo.setReflectionName(name);
        }
        if (type != null) {
          protoReflectionInfo.setType(type);
        }
        if (status != null) {
          protoReflectionInfo.setStatus(status);
        }

        protoReflectionInfo.setNumFailures(num_failures);

        if (sortColumns != null) {
          protoReflectionInfo.setSortColumns(sortColumns);
        }
        if (partitionColumns != null) {
          protoReflectionInfo.setPartitionColumns(partitionColumns);
        }
        if (measures != null) {
          protoReflectionInfo.setMeasures(measures);
        }

        protoReflectionInfo.setArrowCache(arrow_cache);

      return protoReflectionInfo.build();
    }

    public static ReflectionInfo getReflectionInfo(ReflectionDescriptionServiceRPC.ListReflectionsResponse reflectionInfoProto) {
      return new ReflectionInfo(reflectionInfoProto.getReflectionId(),
        reflectionInfoProto.getReflectionName(),
        reflectionInfoProto.getType(),
        reflectionInfoProto.getStatus(),
        reflectionInfoProto.getNumFailures(),
        reflectionInfoProto.getDatasetId(),
        reflectionInfoProto.getDatasetName(),
        reflectionInfoProto.getDatasetType(),
        reflectionInfoProto.getSortColumns(),
        reflectionInfoProto.getPartitionColumns(),
        reflectionInfoProto.getDistributionColumns(),
        reflectionInfoProto.getDimensions(),
        reflectionInfoProto.getMeasures(),
        reflectionInfoProto.getDisplayColumns(),
        reflectionInfoProto.getExternalReflection(),
        reflectionInfoProto.getArrowCache());
    }
  }

  class DependencyInfo {

    public final String reflection_id;
    public final String dependency_id;
    public final String dependency_type;
    public final String dependency_path;

    public DependencyInfo(String reflection_id, String dependency_id, String dependency_type, String dependency_path) {
      this.reflection_id = reflection_id;
      this.dependency_id = dependency_id;
      this.dependency_type = dependency_type;
      this.dependency_path = dependency_path;
    }

    public static DependencyInfo getDependencyInfo(ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse dependencyInfoProto) {
      return new DependencyInfo(dependencyInfoProto.getReflectionId(),
        dependencyInfoProto.getDependencyId(),
        dependencyInfoProto.getDependencyType(),
        dependencyInfoProto.getDependencyPath());
    }

    public ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse toProto() {
      ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse.Builder protoDependencyInfo =
        ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse.newBuilder();

      if (reflection_id != null) {
        protoDependencyInfo.setReflectionId(reflection_id);
      }

      if (dependency_id != null) {
        protoDependencyInfo.setDependencyId(dependency_id);
      }

      if (dependency_type != null) {
        protoDependencyInfo.setDependencyType(dependency_type);
      }

      if (dependency_path != null) {
        protoDependencyInfo.setDependencyPath(dependency_path);
      }

      return protoDependencyInfo.build();
    }
  }

  class MaterializationInfo {

    public final String reflection_id;
    public final String materialization_id;
    public final Timestamp create;
    public final Timestamp expiration;
    public final Long bytes;
    public final Long seriesId;
    public final String init_refresh_job_id;
    public final Integer series_ordinal;
    public final String join_analysis;
    public final String state;
    public final String failure_msg;
    public final String data_partitions;
    public final Timestamp last_refresh_from_pds;
    public final Timestamp last_refresh_finished;
    public final Long last_refresh_duration;


    public MaterializationInfo(String reflection_id, String materialization_id, Timestamp create,
                               Timestamp expiration, Long bytes, Long seriesId, String init_refresh_job_id,
                               Integer series_ordinal, String joinAnalysis, String state, String failureMsg,
                               String dataPartitions, Timestamp lastRefreshFromPds, Timestamp lastRefreshFinished, Long lastRefreshDuration) {
      this.reflection_id = reflection_id;
      this.materialization_id = materialization_id;
      this.create = create;
      this.expiration = expiration;
      this.bytes = bytes;
      this.seriesId = seriesId;
      this.init_refresh_job_id = init_refresh_job_id;
      this.series_ordinal = series_ordinal;
      this.join_analysis = joinAnalysis;
      this.state = state;
      this.failure_msg = failureMsg;
      this.data_partitions = dataPartitions;
      this.last_refresh_from_pds = lastRefreshFromPds;
      this.last_refresh_finished = lastRefreshFinished;
      this.last_refresh_duration = lastRefreshDuration;
    }

    public ReflectionDescriptionServiceRPC.ListMaterializationsResponse toProto() {
      ReflectionDescriptionServiceRPC.ListMaterializationsResponse.Builder protoMaterializationInfo =
        ReflectionDescriptionServiceRPC.ListMaterializationsResponse.newBuilder();

      if(reflection_id != null) {
        protoMaterializationInfo.setReflectionId(reflection_id);
      }

      if(materialization_id != null) {
        protoMaterializationInfo.setMaterializationId(materialization_id);
      }

      if(create != null) {
        protoMaterializationInfo.setCreated(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(create.getTime()).build()
        );
      }

      if (expiration != null) {
        protoMaterializationInfo.setExpires(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(expiration.getTime()).build()
        );
      }

      if (bytes != null) {
        protoMaterializationInfo.setSizeBytes(bytes);
      }

      if (seriesId != null) {
        protoMaterializationInfo.setSeriesId(seriesId);
      }

      if (init_refresh_job_id != null) {
        protoMaterializationInfo.setInitRefreshJobId(init_refresh_job_id);
      }

      if (series_ordinal != null) {
        protoMaterializationInfo.setSeriesOrdinal(series_ordinal);
      }

      if (join_analysis != null) {
        protoMaterializationInfo.setJoinAnalysis(join_analysis);
      }

      if (state != null) {
        protoMaterializationInfo.setState(state);
      }

      if (failure_msg != null) {
        protoMaterializationInfo.setFailureMsg(failure_msg);
      }

      if (data_partitions != null) {
        protoMaterializationInfo.setDataPartitions(data_partitions);
      }

      if (last_refresh_from_pds != null) {
        protoMaterializationInfo.setLastRefreshFromPds(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(last_refresh_from_pds.getTime()));
      }

      if (last_refresh_finished != null) {
        protoMaterializationInfo.setLastRefreshFinished(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(last_refresh_finished.getTime()));
      }

      if (last_refresh_duration != null) {
        protoMaterializationInfo.setLastRefreshDurationMillis(last_refresh_duration);
      }

      return protoMaterializationInfo.build();
    }

    public static MaterializationInfo fromMaterializationInfo(ReflectionDescriptionServiceRPC.ListMaterializationsResponse materializationInfoProto) {
      return new MaterializationInfo(
        materializationInfoProto.getReflectionId(),
        materializationInfoProto.getMaterializationId(),
        new Timestamp(Optional.fromNullable(materializationInfoProto.getCreated().getSeconds()).or(0L)),
        new Timestamp(Optional.fromNullable(materializationInfoProto.getExpires().getSeconds()).or(0L)),
        materializationInfoProto.getSizeBytes(),
        materializationInfoProto.getSeriesId(),
        materializationInfoProto.getInitRefreshJobId(),
        materializationInfoProto.getSeriesOrdinal(),
        materializationInfoProto.getJoinAnalysis(),
        materializationInfoProto.getState(),
        materializationInfoProto.getFailureMsg(),
        materializationInfoProto.getDataPartitions(),
        new Timestamp(Optional.fromNullable(materializationInfoProto.getLastRefreshFromPds().getSeconds()).or(0L)),
        new Timestamp(Optional.fromNullable(materializationInfoProto.getLastRefreshFinished().getSeconds()).or(0L)),
        materializationInfoProto.getLastRefreshDurationMillis()
      );
    }
  }

  class RefreshInfo {
    public final String refresh_id;
    public final String reflection_id;
    public final Long series_id;
    public final Timestamp created_at;
    public final Timestamp modified_at;
    public final String path;
    public final String job_id;
    public final Timestamp job_start;
    public final Timestamp job_end;
    public final Long input_bytes;
    public final Long input_records;
    public final Long output_bytes;
    public final Long output_records;
    // renamed "footprint" to bytes (consistent with MaterializationInfo)
    // as well as we use "foo" pattern in tests that screws up test runs
    // e.g. TestDatasetService.testSearchdatasets
    public final Long bytes;
    public final Double original_cost;
    public final String update_id;
    public final String partitions; // json array of hosts
    public final Integer series_ordinal;

    public RefreshInfo(
        String refresh_id,
        String reflection_id,
        Long series_id,
        Timestamp created_at,
        Timestamp modified_at,
        String path,
        String job_id,
        Timestamp job_start,
        Timestamp job_end,
        Long input_bytes,
        Long input_records,
        Long output_bytes,
        Long output_records,
        Long footprint,
        Double original_cost,
        String update_id,
        String partitions,
        Integer series_ordinal
    ) {
        this.refresh_id = refresh_id;
        this.reflection_id = reflection_id;
        this.series_id = series_id;
        this.created_at = created_at;
        this.modified_at = modified_at;
        this.path = path;
        this.job_id = job_id;
        this.job_start = job_start;
        this.job_end = job_end;
        this.input_bytes = input_bytes;
        this.input_records = input_records;
        this.output_bytes = output_bytes;
        this.output_records = output_records;
        this.bytes = footprint;
        this.original_cost = original_cost;
        this.update_id = update_id;
        this.partitions = partitions;
        this.series_ordinal = series_ordinal;
    }

    public static RefreshInfo fromRefreshInfo(ReflectionDescriptionServiceRPC.GetRefreshInfoResponse refreshInfoProto) {
      return new RefreshInfo(
        refreshInfoProto.getId(),
        refreshInfoProto.getReflectionId(),
        refreshInfoProto.getSeriesId(),
        new Timestamp(Optional.fromNullable(refreshInfoProto.getCreatedAt()).or(0L)),
        new Timestamp(Optional.fromNullable(refreshInfoProto.getModifiedAt()).or(0L)),
        refreshInfoProto.getPath(),
        refreshInfoProto.getJobId(),
        new Timestamp(Optional.fromNullable(refreshInfoProto.getJobStart()).or(0L)),
        new Timestamp(Optional.fromNullable(refreshInfoProto.getJobEnd()).or(0L)),
        refreshInfoProto.getInputBytes(),
        refreshInfoProto.getInputRecords(),
        refreshInfoProto.getOutputBytes(),
        refreshInfoProto.getOutputRecords(),
        refreshInfoProto.getFootprint(),
        refreshInfoProto.getOriginalCost(),
        refreshInfoProto.getUpdateId(),
        refreshInfoProto.getPartition(),
        refreshInfoProto.getSeriesOrdinal()
      );
    }
  }
}
