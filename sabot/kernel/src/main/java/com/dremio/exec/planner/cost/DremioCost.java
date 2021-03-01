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

package com.dremio.exec.planner.cost;

import org.apache.calcite.plan.RelOptUtil;

import com.google.common.base.Objects;

/**
 * Implementation of the RelOptCost, modeled similar to VolcanoCost
 */
public class DremioCost implements RelOptCost {

  /**
   * NOTE: the multiplication factors below are not calibrated yet...these
   * are chosen based on approximations for now. For reference purposes,
   * assume each disk on a server can have a sustained I/O throughput of
   * 100 MBytes/sec.  Suppose there is an array of 16 disks per server..theoretically
   * one could get 1.6GBytes/sec. Suppose network speed is 1GBit/sec which is
   * 128MBytes/sec, although actual transfer rate over the network may be lower.
   * We are only concerned with relative costs, not absolute values.
   * For relative costing, let's assume sending data over the network is
   * about 16x slower than reading/writing to an array of local disks.
   */
  public static final int BASE_CPU_COST = 1;                        // base cpu cost per 'operation'
  public static final int BYTE_DISK_READ_COST = 32 * BASE_CPU_COST;    // disk read cost per byte
  public static final int BYTE_NETWORK_COST = 16 * BYTE_DISK_READ_COST; // network transfer cost per byte


  public static final int SVR_CPU_COST = 8 * BASE_CPU_COST;          // cpu cost for SV remover
  public static final int FUNC_CPU_COST = 12 * BASE_CPU_COST;         // cpu cost for a function evaluation

  // cpu cost for projecting an expression; note that projecting an expression
  // that is not a simple column or constant may include evaluation, but we
  // currently don't model it at that level of detail.
  public static final int PROJECT_CPU_COST = 4 * BASE_CPU_COST;
  public static final double PROJECT_SIMPLE_CPU_COST = 0.00001 * BASE_CPU_COST;

  // hash cpu cost per field (for now we don't distinguish between fields of different types) involves
  // the cost of the following operations:
  // compute hash value, probe hash table, walk hash chain and compare with each element,
  // add to the end of hash chain if no match found
  public static final int HASH_CPU_COST = 8 * BASE_CPU_COST;

  // The ratio to convert memory cost into CPU cost.
  public static final double MEMORY_TO_CPU_RATIO = 1.0;

  public static final int RANGE_PARTITION_CPU_COST = 12 * BASE_CPU_COST;

  // cost of comparing one field with another (ignoring data types for now)
  public static final int COMPARE_CPU_COST = 4 * BASE_CPU_COST;
  public static final int AVG_FIELD_WIDTH = 8;

  public static final double SCAN_CPU_COST_MULTIPLIER = 1;

  // cost of copying data (per cell)
  public static final int COPY_COST = 1 * BASE_CPU_COST;

  public static final long LARGE_ROW_COUNT = 1_000_000_000L;
  public static final long LARGE_FILE_COUNT = 50_000L; // large file count for metadata scan operations

  // Default cost for unknown cost and row count.
  public static final double BIG_ROW_COUNT = 1_000_000_000D;

  /** For the costing formulas in computeSelfCost(), assume the following notations:
  * Let
  *   C = Cost per node.
  *   k = number of fields on which to distribute on
  *   h = CPU cost of computing hash value on 1 field
  *   s = CPU cost of Selection-Vector remover per row
  *   w = Network cost of sending 1 row to 1 destination
  *   c = CPU cost of comparing an incoming row with one on a heap of size N
  */

  static final DremioCost INFINITY =
      new DremioCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        @Override
        public String toString() {
          return "{inf}";
        }
      };

  static final DremioCost HUGE =
      new DremioCost(Double.MAX_VALUE,
        Double.MAX_VALUE,
        Double.MAX_VALUE,
        Double.MAX_VALUE) {
        @Override
        public String toString() {
          return "{huge}";
        }
      };

  static final DremioCost ZERO =
      new DremioCost(0.0, 0.0, 0.0, 0.0) {
        @Override
        public String toString() {
          return "{0}";
        }
      };

  static final DremioCost TINY =
      new DremioCost(1.0, 1.0, 0.0, 0.0) {
        @Override
        public String toString() {
          return "{tiny}";
        }
      };

  final double rowCount;
  final double cpu;
  final double io;
  final double network;
  final double memory;

  public DremioCost(double rowCount, double cpu, double io, double network) {
    this(rowCount, cpu, io, network, 0);
  }

  public DremioCost(double rowCount, double cpu, double io, double network, double memory) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
    this.network = network;
    this.memory = memory;
  }

  @Override
  public double getRows() {
    return rowCount;
  }

  @Override
  public double getCpu() {
    return cpu;
  }

  @Override
  public double getIo() {
    return io;
  }

  @Override
  public double getNetwork() {
    return network;
  }

  public double getMemory() {
    return memory;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(rowCount, cpu, io, network);
  }

  @Override
  public boolean isInfinite() {
    return (this == INFINITY)
      || (this.cpu == Double.POSITIVE_INFINITY)
      || (this.io == Double.POSITIVE_INFINITY)
      || (this.network == Double.POSITIVE_INFINITY)
      || (this.rowCount == Double.POSITIVE_INFINITY)
      || (this.memory == Double.POSITIVE_INFINITY) ;
  }

  @Override
  public boolean equals(org.apache.calcite.plan.RelOptCost other) {
    // here we compare the individual components similar to VolcanoCost, however
    // an alternative would be to add up the components and compare the total.
    // Note that VolcanoPlanner mainly uses isLe() and isLt() for cost comparisons,
    // not equals().
    return this == other
      || (other instanceof DremioCost
          && (this.cpu == ((DremioCost) other).cpu)
          && (this.io == ((DremioCost) other).io)
          && (this.network == ((DremioCost) other).network)
          && (this.rowCount == ((DremioCost) other).rowCount)
          && (this.memory == ((DremioCost) other).memory));
  }

  @Override
  public boolean isEqWithEpsilon(org.apache.calcite.plan.RelOptCost other) {
    if (!(other instanceof DremioCost)) {
      return false;
    }
    DremioCost that = (DremioCost) other;
    return (this == that)
      || ((Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
          && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON)
          && (Math.abs(this.network - that.network) < RelOptUtil.EPSILON)
          && (Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
          && (Math.abs(this.memory - that.memory) < RelOptUtil.EPSILON));
  }

  @Override
  public boolean isLe(org.apache.calcite.plan.RelOptCost other) {
    DremioCost that = (DremioCost) other;

    return this == that
      || this.equals(other)
      || this.isLt(other);
  }

  @Override
  public boolean isLt(org.apache.calcite.plan.RelOptCost other) {
    DremioCost that = (DremioCost) other;

    // Compare the difference of each cost factor one-by-one instead of aggregating them together
    // to minimize issues from double truncation.
    return (that.cpu - this.cpu) +
      (that.io - this.io) +
      (that.network - this.network) +
      (that.memory - this.memory) * DremioCost.MEMORY_TO_CPU_RATIO > 0;
  }

  @Override
  public DremioCost plus(org.apache.calcite.plan.RelOptCost other) {
    DremioCost that = (DremioCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new DremioCost(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io,
        this.network + that.network,
        this.memory + that.memory);
  }

  @Override
  public DremioCost minus(org.apache.calcite.plan.RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    DremioCost that = (DremioCost) other;
    return new DremioCost(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io,
        this.network - that.network,
        this.memory - that.memory);
  }

  @Override
  public DremioCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new DremioCost(rowCount * factor, cpu * factor, io * factor, network * factor, memory * factor);
  }

  @Override
  public double divideBy(org.apache.calcite.plan.RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    DremioCost that = (DremioCost) cost;
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0)
        && !Double.isInfinite(this.rowCount)
        && (that.rowCount != 0)
        && !Double.isInfinite(that.rowCount)) {
      d *= this.rowCount / that.rowCount;
      ++n;
    }
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if ((this.network != 0)
        && !Double.isInfinite(this.network)
        && (that.network != 0)
        && !Double.isInfinite(that.network)) {
      d *= this.network / that.network;
      ++n;
    }

    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  @Override
  public String toString() {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io, " + network + " network, " + memory + " memory}";
  }

  /**
   * Returns the aggregate cost from given {@link org.apache.calcite.plan.RelOptCost} cost.
   *
   */
  public static double aggregateCost(final org.apache.calcite.plan.RelOptCost cost) {
    final double aggCost = DremioCost.BASE_CPU_COST * cost.getCpu() + DremioCost.BYTE_DISK_READ_COST * cost.getIo();

    if (cost instanceof DremioCost) {
      final DremioCost dremioCost = (DremioCost) cost;
      return aggCost + DremioCost.BYTE_NETWORK_COST * dremioCost.getNetwork() + dremioCost.getMemory()  * DremioCost.BASE_CPU_COST / DremioCost.MEMORY_TO_CPU_RATIO;
    }

    return aggCost;
  }

  public static class Factory implements RelOptCostFactory {

    @Override
    public DremioCost makeCost(double dRows, double dCpu, double dIo, double dNetwork, double dMemory) {
      return new DremioCost(dRows, dCpu, dIo, dNetwork, dMemory);
    }

    @Override
    public DremioCost makeCost(double dRows, double dCpu, double dIo, double dNetwork) {
      return new DremioCost(dRows, dCpu, dIo, dNetwork, 0);
    }

    @Override
    public DremioCost makeCost(double dRows, double dCpu, double dIo) {
      return new DremioCost(dRows, dCpu, dIo, 0, 0);
    }

    @Override
    public DremioCost makeHugeCost() {
      return DremioCost.HUGE;
    }

    @Override
    public DremioCost makeInfiniteCost() {
      return DremioCost.INFINITY;
    }

    @Override
    public DremioCost makeTinyCost() {
      return DremioCost.TINY;
    }

    @Override
    public DremioCost makeZeroCost() {
      return DremioCost.ZERO;
    }
  }

}
