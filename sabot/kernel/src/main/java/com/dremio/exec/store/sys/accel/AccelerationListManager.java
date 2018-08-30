/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.exec.proto.ReflectionRPC;
import com.dremio.service.Service;
import com.google.common.base.Optional;

/**
 * Exposes the acceleration store to the execution engine
 */
public interface AccelerationListManager extends Service {

  Iterable<ReflectionInfo> getReflections();
  Iterable<MaterializationInfo> getMaterializations();
  Iterable<RefreshInfo> getRefreshInfos();
  Iterable<DependencyInfo> getReflectionDependencies();

  class ReflectionInfo {
    public final String reflection_id;
    public final String name;
    public final String type;
    public final String status;
    public final int num_failures;
    public final String dataset;
    public final String sortColumns;
    public final String partitionColumns;
    public final String distributionColumns;
    public final String dimensions;
    public final String measures;
    public final String displayColumns;
    public final String externalReflection;

    public ReflectionInfo(String reflectionId, String name, String type, String status, int numFailures, String dataset,
        String sortColumns, String partitionColumns, String distributionColumns, String dimensions, String measures,
        String displayColumns, String externalReflection) {
      this.reflection_id = reflectionId;
      this.name = name;
      this.type = type;
      this.status = status;
      this.num_failures = numFailures;
      this.dataset = dataset;
      this.sortColumns = sortColumns;
      this.partitionColumns = partitionColumns;
      this.distributionColumns = distributionColumns;
      this.dimensions = dimensions;
      this.measures = measures;
      this.displayColumns = displayColumns;
      this.externalReflection = externalReflection;
    }

    public ReflectionRPC.ReflectionInfo toProto() {
      ReflectionRPC.ReflectionInfo.Builder protoReflectionInfo =
        ReflectionRPC.ReflectionInfo.newBuilder();

        if(externalReflection != null) {
          protoReflectionInfo.setExternalReflection(externalReflection);
        }
        if (reflection_id != null) {
          protoReflectionInfo.setReflectionId(reflection_id);
        }
        if(dataset != null) {
          protoReflectionInfo.setDataset(dataset);
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
          protoReflectionInfo.setName(name);
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
      return protoReflectionInfo.build();
    }

    public static ReflectionInfo getReflectionInfo(ReflectionRPC.ReflectionInfo reflectionInfoProto) {
      return new ReflectionInfo(reflectionInfoProto.getReflectionId(),
        reflectionInfoProto.getName(),
        reflectionInfoProto.getType(),
        reflectionInfoProto.getStatus(),
        reflectionInfoProto.getNumFailures(),
        reflectionInfoProto.getDataset(),
        reflectionInfoProto.getSortColumns(),
        reflectionInfoProto.getPartitionColumns(),
        reflectionInfoProto.getDistributionColumns(),
        reflectionInfoProto.getDimensions(),
        reflectionInfoProto.getMeasures(),
        reflectionInfoProto.getDisplayColumns(),
        reflectionInfoProto.getExternalReflection());
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

    public static DependencyInfo getDependencyInfo(ReflectionRPC.DependencyInfo dependencyInfoProto) {
      return new DependencyInfo(dependencyInfoProto.getReflectionId(),
        dependencyInfoProto.getDependencyId(),
        dependencyInfoProto.getDependencyType(),
        dependencyInfoProto.getDependencyPath());
    }

    public ReflectionRPC.DependencyInfo toProto() {
      ReflectionRPC.DependencyInfo.Builder protoDependencyInfo =
        ReflectionRPC.DependencyInfo.newBuilder();

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
    public final String joinAnalysis;
    public final String state;
    public final String failure_msg;
    public final String data_partitions;
    public final Timestamp last_refresh_from_pds;


    public MaterializationInfo(String reflection_id, String materialization_id, Timestamp create,
                               Timestamp expiration, Long bytes, Long seriesId, String init_refresh_job_id,
                               Integer series_ordinal, String joinAnalysis, String state, String failureMsg,
                               String dataPartitions, Timestamp lastRefreshFromPds) {
      this.reflection_id = reflection_id;
      this.materialization_id = materialization_id;
      this.create = create;
      this.expiration = expiration;
      this.bytes = bytes;
      this.seriesId = seriesId;
      this.init_refresh_job_id = init_refresh_job_id;
      this.series_ordinal = series_ordinal;
      this.joinAnalysis = joinAnalysis;
      this.state = state;
      this.failure_msg = failureMsg;
      this.data_partitions = dataPartitions;
      this.last_refresh_from_pds = lastRefreshFromPds;
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
    public final Long update_id;
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
        Long update_id,
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

    public static RefreshInfo fromRefreshInfo(ReflectionRPC.RefreshInfo refreshInfoProto) {
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
