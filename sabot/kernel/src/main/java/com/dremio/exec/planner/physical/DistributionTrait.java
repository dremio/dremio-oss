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
package com.dremio.exec.planner.physical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

public class DistributionTrait implements RelTrait {
  public static enum DistributionType {
    SINGLETON,
    HASH_DISTRIBUTED,
    ADAPTIVE_HASH_DISTRIBUTED,
    RANGE_DISTRIBUTED,
    ROUND_ROBIN_DISTRIBUTED,
    BROADCAST_DISTRIBUTED,
    ANY
  };

  // ANY = can be anything (i.e. it is either unknown or unrestricted)
  public static final DistributionTrait ANY = new DistributionTrait(DistributionType.ANY);
  // SINGLETON = a singleton (i.e. on a fragment)
  public static final DistributionTrait SINGLETON =
      new DistributionTrait(DistributionType.SINGLETON);
  // ROUND_ROBIN = load is distributed across multiple fragments evenly
  public static final DistributionTrait ROUND_ROBIN =
      new DistributionTrait(DistributionType.ROUND_ROBIN_DISTRIBUTED);
  // BROADCAST = sent to all fragments involved
  public static final DistributionTrait BROADCAST =
      new DistributionTrait(DistributionType.BROADCAST_DISTRIBUTED);
  // HASH_DISTRIBUTED = distributed across fragments via hashing the provided fields
  // RANGE_DISTRIBUTED = distributed across fragments by range of the fields

  public static DistributionTrait DEFAULT = ANY;

  private DistributionType type;
  private final ImmutableList<DistributionField> fields;

  private DistributionTrait(DistributionType type) {
    assert (type == DistributionType.SINGLETON
        || type == DistributionType.ANY
        || type == DistributionType.ROUND_ROBIN_DISTRIBUTED
        || type == DistributionType.BROADCAST_DISTRIBUTED);
    this.type = type;
    this.fields = ImmutableList.<DistributionField>of();
  }

  public DistributionTrait(DistributionType type, ImmutableList<DistributionField> fields) {
    assert (type == DistributionType.HASH_DISTRIBUTED
        || type == DistributionType.ADAPTIVE_HASH_DISTRIBUTED
        || type == DistributionType.RANGE_DISTRIBUTED);
    this.type = type;
    this.fields = fields;
  }

  @Override
  public void register(RelOptPlanner planner) {}

  @Override
  public boolean satisfies(RelTrait trait) {

    if (trait instanceof DistributionTrait) {
      DistributionType requiredDist = ((DistributionTrait) trait).getType();
      if (requiredDist == DistributionType.ANY) {
        return true;
      }

      if (this.type == DistributionType.HASH_DISTRIBUTED) {
        if (requiredDist == DistributionType.HASH_DISTRIBUTED) {
          // A subset of the required distribution columns can satisfy (subsume) the requirement
          // e.g: required distribution: {a, b, c}
          // Following can satisfy the requirements: {a}, {b}, {c}, {a, b}, {b, c}, {a, c} or {a, b,
          // c}

          // New: Use equals for subsumes check of hash distribution. If we uses subsumes,
          // a join may end up with hash-distributions using different keys. This would
          // cause incorrect query result.
          return this.equals(trait);
        }
      }
    }

    return this.equals(trait);
  }

  @Override
  public RelTraitDef<DistributionTrait> getTraitDef() {
    return DistributionTraitDef.INSTANCE;
  }

  public DistributionType getType() {
    return this.type;
  }

  public ImmutableList<DistributionField> getFields() {
    return fields;
  }

  @Override
  public int hashCode() {
    return fields == null ? type.hashCode() : type.hashCode() | fields.hashCode() << 4;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DistributionTrait) {
      DistributionTrait that = (DistributionTrait) obj;
      return this.type == that.type && this.fields.equals(that.fields);
    }
    return false;
  }

  @Override
  public String toString() {
    return fields == null ? this.type.toString() : this.type.toString() + "(" + fields + ")";
  }

  public static class DistributionField {
    /** 0-based index of field being DISTRIBUTED. */
    private final int fieldId;

    public DistributionField(int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DistributionField)) {
        return false;
      }
      DistributionField other = (DistributionField) obj;
      return this.fieldId == other.fieldId;
    }

    @Override
    public int hashCode() {
      return this.fieldId;
    }

    public int getFieldId() {
      return this.fieldId;
    }

    @Override
    public String toString() {
      return String.format("[$%s]", this.fieldId);
    }
  }
}
