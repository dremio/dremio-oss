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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;

/**
 * Geo functions
 */
public class GeoFunctions {
  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(com.dremio.exec.expr.fn.impl.GeoFunctions.class);
  @FunctionTemplate(name = "geo_distance", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoDistance implements SimpleFunction {
    @Param
    Float4Holder lat1;
    @Param
    Float4Holder lon1;
    @Param
    Float4Holder lat2;
    @Param
    Float4Holder lon2;
    @Output
    Float8Holder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      double distance = com.dremio.exec.expr.fn.impl.GeoHelper.distance(lat1.value, lon1.value, lat2.value, lon2.value);
      out.value = distance;
    }

  }

  @FunctionTemplate(name = "geo_nearby", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoNearby implements SimpleFunction {
    @Param
    Float4Holder lat1;
    @Param
    Float4Holder lon1;
    @Param
    Float4Holder lat2;
    @Param
    Float4Holder lon2;
    @Param(constant = true)
    Float8Holder distance;
    @Output
    BitHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.value = com.dremio.exec.expr.fn.impl.GeoHelper.isNear(lat1.value, lon1.value, lat2.value, lon2.value, distance.value) ? 1 : 0;
    }

  }

  @FunctionTemplate(name = "geo_beyond", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoBeyond implements SimpleFunction {
    @Param
    Float4Holder lat1;
    @Param
    Float4Holder lon1;
    @Param
    Float4Holder lat2;
    @Param
    Float4Holder lon2;
    @Param(constant = true)
    Float8Holder distance;
    @Output
    BitHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.value = com.dremio.exec.expr.fn.impl.GeoHelper.isNear(lat1.value, lon1.value, lat2.value, lon2.value, distance.value) ? 0 : 1;
    }
  }
  @FunctionTemplate(name = "st_geohash", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoHashEncode implements SimpleFunction {
    @Param
    Float8Holder lat;
    @Param
    Float8Holder lon;
    @Output
    VarCharHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errCtx;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.start = 0;
      out.end = 0;
      String result = com.dremio.exec.expr.fn.impl.GeoHelper.encodeGeohash(lat.value, lon.value, errCtx);
      byte[] byteString = result.getBytes();

      buffer = buffer.reallocIfNeeded(byteString.length);
      out.buffer = buffer;
      out.buffer.setBytes(out.start, byteString);
      out.end = byteString.length;
      out.buffer.readerIndex(out.start);
      out.buffer.writerIndex(byteString.length);

    }
  }
  @FunctionTemplate(name = "st_geohash", nulls = NullHandling.NULL_IF_NULL)
  public static class GeoHashEncodePrecision implements SimpleFunction {
    @Param
    Float8Holder lat;
    @Param
    Float8Holder lon;
    @Param
    BigIntHolder precision;
    @Output
    VarCharHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errCtx;
    @Override
    public void setup() {
    }
    @Override
    public void eval() {
      out.start = 0;
      out.end = 0;
      String result = com.dremio.exec.expr.fn.impl.GeoHelper.encodeGeohash(lat.value, lon.value, precision.value, errCtx);
      byte[] byteString = result.getBytes();

      buffer = buffer.reallocIfNeeded(byteString.length);
      out.buffer = buffer;
      out.buffer.setBytes(out.start, byteString);
      out.end = byteString.length;
      out.buffer.readerIndex(out.start);
      out.buffer.writerIndex(byteString.length);

    }
  }
  @FunctionTemplate(name = "st_fromgeohash", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, derivation = ListOfGeo.class)
  public static class GeoHashDecode implements SimpleFunction {
    @Param
    NullableVarCharHolder encoded;
    @Output
    BaseWriter.ComplexWriter out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errCtx;
    //Cache decode results since clients may need to access struct elements repeatedly.
    @Workspace
    java.util.Map<String, double[]> decodedHashes;
    @Workspace
    int cacheMisses;
    @Workspace
    int cacheHits;
    @Override
    public void setup() {
      decodedHashes = new java.util.HashMap<>();
      cacheMisses = 0;
      cacheHits = 0;
    }
    @Override
    public void eval() {
      final int maxCacheSize = 100;
      if (encoded.end <= encoded.start || encoded.isSet == 0) {
        throw errCtx.error()
          .message(com.dremio.exec.expr.fn.impl.GeoHelper.INVALID_HASH_MSG)
          .build();
      }
      String hash = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(encoded.start,  encoded.end,  encoded.buffer);

      double[] res;
      if (decodedHashes.containsKey(hash)) {
        res = (double[])decodedHashes.get(hash);
        cacheHits++;
      } else {
        cacheMisses++;
        res = com.dremio.exec.expr.fn.impl.GeoHelper.decodeGeohash(hash, errCtx);
        if (decodedHashes.size() <= maxCacheSize) {
          decodedHashes.put(hash, res);
        } else {
          com.dremio.exec.expr.fn.impl.GeoFunctions.logger.debug("Geohash cache has reached the maximum size:" + maxCacheSize);
        }
      }

      if (res.length != 2) {
        //This shouldn't happen, but check the size for safety.
        throw errCtx.error()
          .message("st_fromgeohash computed results in the wrong format")
          .build();
      }
      com.dremio.exec.expr.fn.impl.GeoFunctions.logger.debug("Geohash cache hit/miss:" + cacheHits + "/" + cacheMisses);
      org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter.StructWriter positionWriter = out.rootAsStruct();
      positionWriter.start();
      positionWriter.float8("Latitude").writeFloat8(res[0]);
      positionWriter.float8("Longitude").writeFloat8(res[1]);
      positionWriter.end();
    }
  }
  public static class ListOfGeo implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      List<Field> children = new ArrayList<>();
      children.add(new Field("Latitude", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
      children.add(new Field("Longitude", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
      Field f = new Field("Results",  FieldType.notNullable(new ArrowType.Struct()), children);
      return CompleteType.fromField(f);
    }
  }
}
