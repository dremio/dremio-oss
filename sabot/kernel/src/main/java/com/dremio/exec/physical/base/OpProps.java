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
package com.dremio.exec.physical.base;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.service.users.SystemUser;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

/**
 * Collection of properties applicable to all operators.
 */
@JsonInclude(Include.NON_NULL)
public class OpProps {

  private final int operatorId;
  private final String userName;
  private final double cost;
  private final boolean singleStream;
  private final int targetBatchSize;
  private final BatchSchema schema;
  private final int schemaHashCode;
  private final boolean memoryBound;
  private final double memoryFactor;
  private final boolean memoryExpensive;
  private final long memLowLimit;

  // this is currently mutable but should ultimately become immutable.
  private long memLimit;
  private long memReserve;
  private long forcedMemLimit;

  public OpProps(
    int operatorId,
    String userName,
    long memReserve,
    long memLimit,
    long memLowLimit,
    double cost,
    boolean singleStream,
    int targetBatchSize,
    BatchSchema schema,
    boolean memoryBound,
    double memoryFactor,
    boolean memoryExpensive) {
    this(operatorId, userName, memReserve, memLimit, memLowLimit, 0, cost, singleStream,
      targetBatchSize, schema, memoryBound, memoryFactor, memoryExpensive);
  }

  public OpProps(
    int operatorId,
    String userName,
    long memReserve,
    long memLimit,
    long memLowLimit,
    long forcedMemLimit,
    double cost,
    boolean singleStream,
    int targetBatchSize,
    BatchSchema schema,
    boolean memoryBound,
    double memoryFactor,
    boolean memoryExpensive) {
    super();
    this.operatorId = operatorId;
    this.userName = userName;
    this.memReserve = memReserve;
    this.memLimit = memLimit;
    this.memLowLimit = memLowLimit;
    this.forcedMemLimit = forcedMemLimit;
    this.cost = cost;
    this.singleStream = singleStream;
    this.targetBatchSize = targetBatchSize;
    this.schema = schema;
    this.schemaHashCode = schema.clone(SelectionVectorMode.NONE).toByteString().hashCode();
    this.memoryBound = memoryBound;
    this.memoryFactor = memoryFactor;
    this.memoryExpensive = memoryExpensive;
  }

  @JsonCreator
  public OpProps(
    @JsonProperty("operatorId") int operatorId,
    @JsonProperty("userName") String userName,
    @JsonProperty("memReserve") long memReserve,
    @JsonProperty("memLimit") long memLimit,
    @JsonProperty("cost") double cost,
    @JsonProperty("singleStream") boolean singleStream,
    @JsonProperty("targetBatchSize") int targetBatchSize,
    @JsonProperty("schemaHashCode") int schemaHashCode) {
    super();
    this.operatorId = operatorId;
    this.userName = userName;
    this.memReserve = memReserve;
    this.memLimit = memLimit;
    this.memLowLimit = 0;
    this.forcedMemLimit = 0;
    this.cost = cost;
    this.singleStream = singleStream;
    this.targetBatchSize = targetBatchSize;
    this.memoryExpensive = false;
    this.schema = null;
    this.schemaHashCode = schemaHashCode;
    this.memoryBound = false;
    this.memoryFactor = 1.0d;
  }

  public String getUserName() {
    return userName;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public long getMemReserve() {
    return memReserve;
  }

  public long getMemLimit() {
    return memLimit;
  }

  public long getForcedMemLimit() {
    return forcedMemLimit;
  }

  @JsonIgnore
  public long getMemLowLimit() {
    return memLowLimit;
  }

  @JsonIgnore
  public boolean isMemoryExpensive() {
    return memoryExpensive;
  }

  public double getCost() {
    return cost;
  }

  public int getSchemaHashCode() {
    return schemaHashCode;
  }

  public boolean isSingleStream() {
    return singleStream;
  }

  public int getTargetBatchSize() {
    return targetBatchSize;
  }

  public void setMemLimit(long memLimit) {
    this.memLimit = memLimit;
  }

  @JsonIgnore // do not serialize within props. If an operator wants, must serialize.
  public BatchSchema getSchema() {
    return schema;
  }

  @JsonIgnore
  public int getMajorFragmentId() { return getMajorFragmentId(operatorId); }

  @JsonIgnore
  public int getLocalOperatorId() { return getLocalOperatorId(operatorId); }

  public static int buildOperatorId(int majorFragmentId, int localOperatorId) {
    return (majorFragmentId << 16) + localOperatorId;
  }

  public static int getMajorFragmentId(int operatorId) {
    return operatorId >> 16;
  }

  public static int getLocalOperatorId(int operatorId) {
    return Short.MAX_VALUE & operatorId;
  }

  /**
   * Whether or not this operation should have a limited memory consumption envelope. Sometimes an
   * operation should be considered for calculation purposes but not actually bounded since the
   * moment it hit a bound, it would die anyway. As memory-bounding is allowed in more operations,
   * they can override this method to return true.
   *
   * @return true if operation should be limited.
   */
  @JsonIgnore
  public boolean isMemoryBound() {
    return memoryBound;
  }

  /**
   * How much to weight this particular operator within memory calculations. Must be greater than or
   * equal to zero.
   *
   * @return 1.0 for average weight, >1 should get more than average, <1 to get less than average.
   *         If set to 0, this means this operator shouldn't be considered for factoring.
   */
  @JsonIgnore
  public double getMemoryFactor() {
    return memoryFactor;
  }

  public OpProps cloneWithNewReserve(long newReserve) {
    return new OpProps(operatorId, userName, newReserve, memLimit, memLowLimit, forcedMemLimit, cost,
      singleStream, targetBatchSize, schema, memoryBound, memoryFactor, memoryExpensive);
  }

  public OpProps cloneWithMemoryFactor(double memoryFactor) {
    return new OpProps(operatorId, userName, memReserve, memLimit, memLowLimit, forcedMemLimit, cost,
      singleStream, targetBatchSize, schema, memoryBound, memoryFactor, memoryExpensive);
  }

  public OpProps cloneWithBound(boolean bounded) {
    return new OpProps(operatorId, userName, memReserve, memLimit, memLowLimit, forcedMemLimit, cost,
      singleStream, targetBatchSize, schema, bounded, memoryFactor, memoryExpensive);
  }

  public OpProps cloneWithMemoryExpensive(boolean memExpensive) {
    return new OpProps(operatorId, userName, memReserve, memLimit, memLowLimit, forcedMemLimit, cost,
      singleStream, targetBatchSize, schema, memoryBound, memoryFactor, memExpensive);
  }

  public OpProps cloneWithNewBatchSize(int targetBatchSize) {
    return new OpProps(operatorId, userName, memReserve, memLimit, memLowLimit, forcedMemLimit, cost,
      singleStream, targetBatchSize, schema, memoryBound, memoryFactor, memoryExpensive);
  }

  public OpProps cloneWithNewIdAndSchema(int id, BatchSchema schema) {
    return new OpProps(id, userName, memReserve, memLimit, memLowLimit, forcedMemLimit, cost,
      singleStream, targetBatchSize, schema, memoryBound, memoryFactor, memoryExpensive);
  }

  public static OpProps prototype(int operatorId, long reserve, long limit) {
    return new OpProps(operatorId, SystemUser.SYSTEM_USERNAME, reserve, limit, 0, 1, false, 1024, new BatchSchema(ImmutableList.of()), false, 1.0d, false);
  }

  public static OpProps prototype(long reserve, long limit) {
    return  prototype(0, reserve, limit);
  }

  public static OpProps prototype(int operatorId) {
    return prototype(operatorId, 1_000_000, Long.MAX_VALUE);
  }

  public static OpProps prototype() {
    return prototype(1_000_000, Long.MAX_VALUE);
  }

  public void setMemReserve(long reserve) {
    this.memReserve = reserve;
  }
}
