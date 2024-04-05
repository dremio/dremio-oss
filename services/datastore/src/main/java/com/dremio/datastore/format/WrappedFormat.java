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
package com.dremio.datastore.format;

import com.dremio.datastore.Converter;
import com.dremio.datastore.DatastoreFatalException;
import com.dremio.datastore.FormatVisitor;

/**
 * A format that converts some non-supported type into a supported one.
 *
 * @param <OUTER> The outside type visible to the KVStore user
 * @param <INNER> The inner type used internally for serialization. Not visible to the user
 */
public final class WrappedFormat<OUTER, INNER> implements Format<OUTER> {
  private final Class<OUTER> clazz;
  private final Format<INNER> nestedFormat;
  private final Converter<OUTER, INNER> in2out;

  WrappedFormat(Class<OUTER> clazz, Format<INNER> innerFormat, Converter<OUTER, INNER> in2out) {
    this.clazz = clazz;
    this.nestedFormat = innerFormat;
    this.in2out = in2out;
  }

  @Override
  public Class<OUTER> getRepresentedClass() {
    return clazz;
  }

  @Override
  public <RET> RET apply(FormatVisitor<RET> visitor) throws DatastoreFatalException {

    return visitor.visitWrappedFormat(clazz, in2out, nestedFormat);
  }
}
