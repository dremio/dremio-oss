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
package com.dremio.exec.planner.sql.handlers.refresh;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.SqlNodeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DateTimes;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.implicit.DecimalTools;
import com.dremio.exec.util.PartitionUtils;
import com.google.common.base.Preconditions;

/**
 * Validator for validating refresh dataset command
 */
public class RefreshDatasetValidator {
  private static final Logger logger = LoggerFactory.getLogger(RefreshDatasetValidator.class);

  private final UnlimitedSplitsMetadataProvider metadataProvider;

  protected List<PartitionValue> partitionValues = new ArrayList<>();

  public RefreshDatasetValidator(UnlimitedSplitsMetadataProvider provider) {
    this.metadataProvider = provider;
  }

  public List<PartitionValue> getPartitionValues() {
    return partitionValues;
  }

  public void validate(SqlRefreshDataset sqlNode) {
    // Validate if refresh dataset command is run on all the partition columns
    SqlNodeList sqlNodes = sqlNode.getPartitionList();
    int partitionListSize = sqlNodes == null ? 0 : sqlNodes.size();

    List<String> partitionColumns = metadataProvider.getPartitionColumns();
    if(partitionColumns == null || partitionColumns.size() == 0){
      return;
    }

    // columns that follow the 'dirN' naming
    Set<String> dirPartitionColumnsSet = partitionColumns.stream()
            .filter(col -> !IncrementalUpdateUtils.UPDATE_COLUMN.equals(col)) // Ignore DREMIO_UPDATE_COLUMN
            .filter(col -> PartitionUtils.isPartitionName(col, true))
            .collect(Collectors.toSet());

    // columns that do not follow the 'dirN' naming
    Set<String> labelPartitionColumnsSet = partitionColumns.stream()
            .filter(col -> !IncrementalUpdateUtils.UPDATE_COLUMN.equals(col)) // Ignore DREMIO_UPDATE_COLUMN
            .filter(col -> !PartitionUtils.isPartitionName(col, true))
            .collect(Collectors.toSet());

    boolean inferredPartionsExist = false;
    if(dirPartitionColumnsSet.size() > 0 && labelPartitionColumnsSet.size() > 0){
      inferredPartionsExist = true;
    }

    int partitionColumnsSize = 0;
    if(dirPartitionColumnsSet.size() > 0){
      partitionColumnsSize = dirPartitionColumnsSet.size();
    } else if(labelPartitionColumnsSet.size() > 0) {
      // hive (+others) may not use dirN naming for partitions, so partitions appear in labelPartitionColumns
      partitionColumnsSize = labelPartitionColumnsSet.size();
    }

    Preconditions.checkArgument(
      partitionListSize == partitionColumnsSize,
      "Refresh dataset command must include all partitions.");

    if (partitionListSize == 0) {
      // Nothing to validate
      return;
    }

    // Verify that partitionNames are either all 'dirN' or all 'label'
    boolean dirNPartitionNames = false;
    boolean labelPartitionNames = false;

    // Validate each column is a partition column
    Map<String, String> partitionKVMap = sqlNode.getPartition();
    for (String partitionCol : partitionKVMap.keySet()) {
      if(dirPartitionColumnsSet.contains(partitionCol)) {
        if(labelPartitionNames){
          throw UserException.validationError()
                  .message(String.format("Partition columns should be all 'dirN' columns or all inferred partition columns: %s",
                          metadataProvider.getPartitionColumns()))
                  .build(logger);
        }
        dirNPartitionNames = true;

      } else if(labelPartitionColumnsSet.contains(partitionCol)) {
        if(dirNPartitionNames){
          throw UserException.validationError()
                  .message(String.format("Partition columns should be all 'dirN' columns or all inferred partition columns: %s",
                          metadataProvider.getPartitionColumns()))
                  .build(logger);
        }
        labelPartitionNames = true;

      } else {
        throw UserException.validationError()
                .message(String.format("Column '%s' not found in the list of partition columns: %s", partitionCol,
                        dirNPartitionNames ? dirPartitionColumnsSet : labelPartitionColumnsSet ))
                .build(logger);
      }
    }

    BatchSchema batchSchema = metadataProvider.getTableSchema();
    partitionValues = convertToPartitionValue(partitionKVMap, batchSchema, labelPartitionNames && inferredPartionsExist);
  }

  /**
   * Given a partition columns key-value string map and a batch schema,
   * convert the partition values to their corresponding types from the
   * batch schema.
   *
   * @param partitionKVMap Partition columns key-value string map
   * @param batchSchema    The given batch schema for type matching
   * @return Converted list of partition columns as PartitionValue
   */
  public static List<PartitionValue> convertToPartitionValue(Map<String, String> partitionKVMap,
                                                             BatchSchema batchSchema,
                                                             boolean inferredPartitionValues) {
    final List<Field> fields = batchSchema.getFields();
    final List<PartitionValue> partitionValues = new ArrayList<>();
    for (Field field : fields) {
      String fieldName = field.getName();
      String partitionColValue = partitionKVMap.get(fieldName);
      if (partitionColValue != null) {
        // Convert each partition column into PartitionValue
        if(inferredPartitionValues){
          // reconstruct the original partition column from the inferred values
          partitionColValue = String.format("%s=%s", fieldName, partitionColValue);
          PartitionValue partitionValue = getPartitionValue(field, fieldName, partitionColValue);
          partitionValues.add(partitionValue);
        } else {
          PartitionValue partitionValue = getPartitionValue(field, fieldName, partitionColValue);
          partitionValues.add(partitionValue);
        }
      }
    }

    return partitionValues;
  }

  private static PartitionValue getPartitionValue(Field field, String name, String value) {
    final ArrowType arrowType = field.getFieldType().getType();

    switch (arrowType.getTypeID()) {
      case Binary: {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return PartitionValue.of(name, ByteBuffer.wrap(bytes));
      }

      case Bool: {
        return PartitionValue.of(name, Boolean.parseBoolean(value));
      }

      case FloatingPoint: {
        final FloatingPointPrecision precision = ((ArrowType.FloatingPoint) arrowType).getPrecision();
        switch (precision) {
          case SINGLE:
            try {
              return PartitionValue.of(name, Float.parseFloat(value));
            } catch (NumberFormatException ex) {
              throw new RuntimeException(String.format("Unable to parse floating value: %s", value), ex);
            }
          case DOUBLE:
            try {
              return PartitionValue.of(name, Double.parseDouble(value));
            } catch (NumberFormatException ex) {
              throw new RuntimeException(String.format("Unable to parse double value: %s", value), ex);
            }
          default:
            throw new IllegalStateException("Unsupported precision: " + precision);
        }
      }

      case Int: {
        final int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
        switch (bitWidth) {
          case 32:
            try {
              return PartitionValue.of(name, Integer.parseInt(value));
            } catch (NumberFormatException ex) {
              throw new RuntimeException(String.format("Unable to parse integer value: %s", value), ex);
            }
          case 64:
            try {
              return PartitionValue.of(name, Long.parseLong(value));
            } catch (NumberFormatException ex) {
              throw new RuntimeException(String.format("Unable to parse long value: %s", value), ex);
            }
          default:
            throw new IllegalStateException("Unsupported bitWith: " + bitWidth);
        }
      }

      case Utf8: {
        return PartitionValue.of(name, value);
      }

      case Decimal: {
        final ArrowType.Decimal decimalTypeInfo = (ArrowType.Decimal) arrowType;
        final int precision = decimalTypeInfo.getPrecision();
        if (precision > 38) {
          throw UserException.unsupportedError()
            .message("Dremio only supports decimals up to 38 digits in precision. This table has a partition value with scale of %d digits.", precision)
            .build(logger);
        }
        try {
          final BigDecimal original = new BigDecimal(value);
          // we can't just use unscaledValue() since BigDecimal doesn't store trailing zeroes and we need to ensure decoding includes the correct scale.
          final BigInteger unscaled = original.movePointRight(decimalTypeInfo.getScale()).unscaledValue();
          return PartitionValue.of(name, ByteBuffer.wrap(DecimalTools.signExtend16(unscaled.toByteArray())));
        } catch (NumberFormatException ex) {
          throw new RuntimeException(String.format("Unable to parse decimal value: %s", value), ex);
        }
      }

      case Date: {
        return PartitionValue.of(name, DateTimes.toJavaTimeMillisFromJdbcDate(value));
      }

      case Timestamp: {
        final TimeUnit timeUnit = ((ArrowType.Timestamp) arrowType).getUnit();
        if (timeUnit == TimeUnit.MILLISECOND) {
          return PartitionValue.of(name, DateTimes.toJavaTimeMillisFromJdbcTimestamp(value));
        } else {
          throw new RuntimeException(String.format("Time unit must be in milliseconds but was %s", timeUnit));
        }
      }

      default: {
        throw new IllegalStateException("Unsupported field type: " + arrowType);
      }
    }
  }
}
