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
package com.dremio.exec.expr.fn.impl;

import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

/**
 * Geo functions
 */
public class GeoFunctions {
  public static final SqlFunction GEO_DISTANCE = new SqlFunction(
      "GEO_DISTANCE",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.DOUBLE,
      null,
      new DremioArgChecker(
          true,
          DremioArgChecker.ofFloat("lat1_deg"),
          DremioArgChecker.ofFloat("lon1_deg"),
          DremioArgChecker.ofFloat("lat2_deg"),
          DremioArgChecker.ofFloat("lon2_deg")
          ),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);

  public static final SqlFunction GEO_NEARBY = new SqlFunction(
      "GEO_NEARBY",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      new DremioArgChecker(
          false,
          DremioArgChecker.ofFloat("lat1_deg"),
          DremioArgChecker.ofFloat("lon1_deg"),
          DremioArgChecker.ofFloat("lat2_deg"),
          DremioArgChecker.ofFloat("lon2_deg"),
          DremioArgChecker.ofDouble("distance_meters")
          ),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);

  public static final SqlFunction GEO_BEYOND = new SqlFunction(
      "GEO_BEYOND",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      new DremioArgChecker(
          false,
          DremioArgChecker.ofFloat("lat1_deg"),
          DremioArgChecker.ofFloat("lon1_deg"),
          DremioArgChecker.ofFloat("lat2_deg"),
          DremioArgChecker.ofFloat("lon2_deg"),
          DremioArgChecker.ofDouble("distance_meters")
          ),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);

  @FunctionTemplate(name = "geo_distance", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoDistance implements SimpleFunction {
    @Param Float4Holder lat1;
    @Param Float4Holder lon1;
    @Param Float4Holder lat2;
    @Param Float4Holder lon2;
    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
      double distance = com.dremio.exec.expr.fn.impl.GeoHelper.distance(lat1.value,lon1.value,lat2.value,lon2.value);
      out.value = distance;
    }

  }

  @FunctionTemplate(name = "geo_nearby", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoNearby implements SimpleFunction {
    @Param Float4Holder lat1;
    @Param Float4Holder lon1;
    @Param Float4Holder lat2;
    @Param Float4Holder lon2;
    @Param(constant = true) Float8Holder distance;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = com.dremio.exec.expr.fn.impl.GeoHelper.isNear(lat1.value, lon1.value, lat2.value, lon2.value, distance.value) ? 1 : 0;
    }

  }

  @FunctionTemplate(name = "geo_beyond", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoBeyond implements SimpleFunction {
    @Param Float4Holder lat1;
    @Param Float4Holder lon1;
    @Param Float4Holder lat2;
    @Param Float4Holder lon2;
    @Param(constant = true) Float8Holder distance;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = com.dremio.exec.expr.fn.impl.GeoHelper.isNear(lat1.value, lon1.value, lat2.value, lon2.value, distance.value) ? 0 : 1;
    }

  }
}
