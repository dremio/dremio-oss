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
package com.dremio.exec.store.parquet;

import com.dremio.common.types.TypeProtos;

public class FieldInfo {
  final String parquetType;
  final String name;
  final int bitLength;
  final int numberOfPages;
  final Object[] values;
  final TypeProtos.MinorType type;

  FieldInfo(String parquetType, String name, int bitLength, Object[] values,
      TypeProtos.MinorType type, ParquetTestProperties props){
    this.parquetType = parquetType;
    this.name = name;
    this.bitLength  = bitLength;
    this.numberOfPages = Math.max(1,
        (int) Math.ceil( ((long) props.recordsPerRowGroup) * bitLength / 8.0 / props.bytesPerPage));

    // generator is designed to use 3 values
    if (values.length != 3) {
      throw new IllegalStateException("generator is designed to use 3 values");
    }
    this.values = values;

    this.type = type;
  }
}
