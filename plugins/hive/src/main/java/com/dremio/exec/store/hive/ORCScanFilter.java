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
package com.dremio.exec.store.hive;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;

import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.store.ScanFilter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Implementation of {@link ScanFilter} for Hive-ORC (Optimized Row Columnar format files) scan
 */
@JsonTypeName("ORCScanFilter")
public class ORCScanFilter implements ScanFilter {

  private final SearchArgument sarg;

  @JsonCreator
  public ORCScanFilter(@JsonProperty("kryoBase64EncodedFilter") final String kryoBase64EncodedFilter) {
    Preconditions.checkNotNull(kryoBase64EncodedFilter);
    try (Input input = new Input(Base64.decodeBase64(kryoBase64EncodedFilter))) {
      this.sarg = new Kryo().readObject(input, SearchArgumentImpl.class);
    }
  }

  public ORCScanFilter(final SearchArgument sarg) {
    Preconditions.checkNotNull(sarg, "expected a non-null filter expression");
    this.sarg = sarg;
  }

  @JsonProperty("kryoBase64EncodedFilter")
  public String getKryoBase64EncodedFilter() {
    try(Output out = new Output(4 * 1024, 10 * 1024 * 1024)) {
      new Kryo().writeObject(out, sarg);
      out.flush();
      return Base64.encodeBase64String(out.toBytes());
    }
  }

  @JsonIgnore
  public SearchArgument getSarg() {
    return sarg;
  }

  @Override
  public double getCostAdjustment() {
    return ScanRelBase.DEFAULT_COST_ADJUSTMENT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ORCScanFilter that = (ORCScanFilter) o;
    return Objects.equal(sarg, that.sarg);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sarg);
  }

  @Override
  public String toString() {
    return "filterExpr = [" + sarg + "]";
  }
}
