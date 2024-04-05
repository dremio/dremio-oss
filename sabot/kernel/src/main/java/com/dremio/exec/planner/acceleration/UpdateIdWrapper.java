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
package com.dremio.exec.planner.acceleration;

import com.dremio.common.types.MinorType;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.proto.model.MultiDatasetUpdateId;
import com.dremio.proto.model.SingleDatasetUpdateId;
import com.dremio.proto.model.UpdateId;
import java.math.BigDecimal;
import java.util.stream.Collectors;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper class for incremental refresh update id */
public class UpdateIdWrapper {
  private static final Logger logger = LoggerFactory.getLogger(UpdateIdWrapper.class);
  public static final Serializer<UpdateId, byte[]> UPDATE_ID_ABSTRACT_SERIALIZER =
      ProtostuffSerializer.of(UpdateId.getSchema());

  private UpdateId updateId;

  public UpdateIdWrapper() {
    this.updateId = new UpdateId();
  }

  public UpdateIdWrapper(MinorType type) {
    updateId = new UpdateId();
    if (type != null) {
      setType(type);
    }
  }

  public UpdateIdWrapper(UpdateId updateId) {
    this.updateId = updateId;
    MinorType type = updateId.getType();
    if (type != null) {
      setType(type);
    }
  }

  public MinorType getType() {
    return updateId.getType();
  }

  public void setType(MinorType type) {
    updateId.setType(type);
  }

  public UpdateId getUpdateId() {
    return updateId;
  }

  public void update(BigDecimal updateValue) {
    setType(MinorType.DECIMAL);
    if (updateValue != null) {
      if (updateId.getStringUpdateId() == null
          || new BigDecimal(updateId.getStringUpdateId()).compareTo(updateValue) < 0) {
        updateId.setStringUpdateId(updateValue.toString());
      }
    }
  }

  public void update(String updateValue) {
    setType(MinorType.VARCHAR);
    if (updateValue != null) {
      if ((updateId.getStringUpdateId() == null)
          || (updateId.getStringUpdateId().compareTo(updateValue) < 0)) {
        updateId.setStringUpdateId(updateValue);
      }
    }
  }

  public void update(Float updateValue) {
    setType(MinorType.FLOAT4);
    if (updateValue != null) {
      if ((updateId.getFloatUpdateId() == null) || (updateValue > updateId.getFloatUpdateId())) {
        updateId.setFloatUpdateId(updateValue);
      }
    }
  }

  public void update(Long updateValue, MinorType updateType) {
    setType(updateType);
    if (updateValue != null) {
      if ((updateId.getLongUpdateId() == null) || (updateValue > updateId.getLongUpdateId())) {
        updateId.setLongUpdateId(updateValue);
      }
    }
  }

  public void update(Integer updateValue, MinorType updateType) {
    setType(updateType);
    if (updateValue != null) {
      if ((updateId.getIntUpdateId() == null) || (updateValue > updateId.getIntUpdateId())) {
        updateId.setIntUpdateId(updateValue);
      }
    }
  }

  public void update(Double updateValue) {
    setType(MinorType.FLOAT8);
    if (updateValue != null) {
      if ((updateId.getDoubleUpdateId() == null) || (updateValue > updateId.getDoubleUpdateId())) {
        updateId.setDoubleUpdateId(updateValue);
      }
    }
  }

  public Object getObjectValue() {
    MinorType type = updateId.getType();
    if (type != null) {
      switch (type) {
        case FLOAT4:
          return updateId.getFloatUpdateId();
        case FLOAT8:
          return updateId.getDoubleUpdateId();
        case VARCHAR:
          return updateId.getStringUpdateId();
        case DECIMAL:
          if (updateId.getStringUpdateId() != null) {
            return new BigDecimal(updateId.getStringUpdateId());
          }
          break;
        case DATE:
        case INT:
          return updateId.getIntUpdateId();
        case BIGINT:
        case TIMESTAMP:
          return updateId.getLongUpdateId();
        default:
      }
    }
    return null;
  }

  public void update(UpdateId updateValue) {
    setType(updateValue.getType());
    switch (updateValue.getType()) {
      case FLOAT4:
        update(updateValue.getFloatUpdateId());
        break;
      case FLOAT8:
        update(updateValue.getDoubleUpdateId());
        break;
      case VARCHAR:
        update(updateValue.getStringUpdateId());
        break;
      case DECIMAL:
        if (updateValue.getStringUpdateId() != null) {
          update(new BigDecimal(updateValue.getStringUpdateId()));
        }
        break;
      case INT:
        update(updateValue.getIntUpdateId(), MinorType.INT);
        break;
      case TIMESTAMP:
        update(updateValue.getLongUpdateId(), MinorType.TIMESTAMP);
        break;
      case DATE:
        update(updateValue.getIntUpdateId(), MinorType.DATE);
        break;
      case BIGINT:
        update(updateValue.getLongUpdateId(), MinorType.BIGINT);
        break;
      default:
    }
  }

  public String toStringValue() {
    if (updateId.getType() != null) {
      switch (updateId.getType()) {
        case FLOAT4:
          return String.valueOf(updateId.getFloatUpdateId());
        case FLOAT8:
          return String.valueOf(updateId.getDoubleUpdateId());
        case DECIMAL:
        case VARCHAR:
          return updateId.getStringUpdateId();
        case INT:
        case DATE:
          return String.valueOf(updateId.getIntUpdateId());
        case TIMESTAMP:
        case BIGINT:
          return String.valueOf(updateId.getLongUpdateId());
        default:
      }
    }
    if (updateId.getUpdateIdType() == UpdateId.IdType.MULTI_DATASET) {
      return toUpdateIdString(updateId.getMultiDatasetUpdateId());
    }
    return "";
  }

  /**
   * Generate a string to display in the UI for "select * from sys.refreshes" for a
   * MultiDatasetUpdateId
   */
  private String toUpdateIdString(MultiDatasetUpdateId multiDatasetUpdateId) {
    if (multiDatasetUpdateId != null
        && multiDatasetUpdateId.getSingleDatasetUpdateIdList() != null) {
      return updateId.getMultiDatasetUpdateId().getSingleDatasetUpdateIdList().stream()
          .map(this::toUpdateIdString)
          .collect(Collectors.joining(", "));
    }
    return "";
  }

  /**
   * Generate a string to display in the UI for "select * from sys.refreshes" for a
   * SingleDatasetUpdateId
   */
  private String toUpdateIdString(SingleDatasetUpdateId singleDatasetUpdateId) {
    if (singleDatasetUpdateId == null) {
      return "null";
    }
    return ((singleDatasetUpdateId.getDatasetId() != null
            ? singleDatasetUpdateId.getDatasetId()
            : "null")
        + ":"
        + (singleDatasetUpdateId.getSnapshotId() != null
            ? singleDatasetUpdateId.getSnapshotId()
            : "null"));
  }

  public byte[] serialize() {
    return UPDATE_ID_ABSTRACT_SERIALIZER.serialize(updateId);
  }

  public static UpdateId deserialize(byte[] bytes) {
    return UPDATE_ID_ABSTRACT_SERIALIZER.deserialize(bytes);
  }

  public static MinorType getMinorTypeFromSqlTypeName(SqlTypeName type) {
    switch (type) {
      case FLOAT:
        return MinorType.FLOAT4;
      case DOUBLE:
        return MinorType.FLOAT8;
      case DECIMAL:
        return MinorType.DECIMAL;
      case VARCHAR:
        return MinorType.VARCHAR;
      case INTEGER:
        return MinorType.INT;
      case BIGINT:
        return MinorType.BIGINT;
      case TIMESTAMP:
        return MinorType.TIMESTAMP;
      case DATE:
        return MinorType.DATE;
      default:
        return null;
    }
  }
}
