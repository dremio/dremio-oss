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
package com.dremio.common.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class FileSizeUnitTest {

  @Test
  void shouldReportUnitAsBytes() {
    assertThat(FileSizeUnit.BYTE.getSizeInBytes()).isEqualTo(1L);
    assertThat(FileSizeUnit.KB.getSizeInBytes()).isEqualTo(1024L);
    assertThat(FileSizeUnit.MB.getSizeInBytes()).isEqualTo(1024L * 1024);
    assertThat(FileSizeUnit.GB.getSizeInBytes()).isEqualTo(1024L * 1024 * 1024);
    assertThat(FileSizeUnit.TB.getSizeInBytes()).isEqualTo(1024L * 1024 * 1024 * 1024);
    assertThat(FileSizeUnit.PB.getSizeInBytes()).isEqualTo(1024L * 1024 * 1024 * 1024 * 1024);
  }

  @Test
  void shouldConvertFromBytesToUnit() {
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.BYTE)).isEqualTo(1L);
    assertThat(FileSizeUnit.toUnit(1024L, FileSizeUnit.KB)).isEqualTo(1L);
    assertThat(FileSizeUnit.toUnit(1024L * 1024, FileSizeUnit.MB)).isEqualTo(1L);
    assertThat(FileSizeUnit.toUnit(1024L * 1024 * 1024, FileSizeUnit.GB)).isEqualTo(1L);
    assertThat(FileSizeUnit.toUnit(1024L * 1024 * 1024 * 1024, FileSizeUnit.TB)).isEqualTo(1L);
    assertThat(FileSizeUnit.toUnit(1024L * 1024 * 1024 * 1024 * 1024, FileSizeUnit.PB))
        .isEqualTo(1L);

    assertThat(FileSizeUnit.toUnit(1536L, FileSizeUnit.KB))
        .isEqualTo(2L); // 1.5 KB rounds up to 2 KB
    assertThat(FileSizeUnit.toUnit(5L * 1024, FileSizeUnit.KB)).isEqualTo(5L);
    assertThat(FileSizeUnit.toUnit(3L * 1024 * 1024, FileSizeUnit.MB)).isEqualTo(3L);
    assertThat(FileSizeUnit.toUnit(8L * 1024 * 1024 * 1024, FileSizeUnit.GB)).isEqualTo(8L);
    assertThat(FileSizeUnit.toUnit(7L * 1024 * 1024 * 1024 * 1024, FileSizeUnit.TB)).isEqualTo(7L);
  }

  @Test
  void shouldConvertZeroValues() {
    assertThat(FileSizeUnit.toUnit(0L, FileSizeUnit.BYTE)).isEqualTo(0L);
    assertThat(FileSizeUnit.toUnit(0L, FileSizeUnit.KB)).isEqualTo(0L);
    assertThat(FileSizeUnit.toUnit(0L, FileSizeUnit.MB)).isEqualTo(0L);
    assertThat(FileSizeUnit.toUnit(0L, FileSizeUnit.GB)).isEqualTo(0L);
    assertThat(FileSizeUnit.toUnit(0L, FileSizeUnit.TB)).isEqualTo(0L);
    assertThat(FileSizeUnit.toUnit(0L, FileSizeUnit.PB)).isEqualTo(0L);
  }

  @Test
  void testConvertToLowerUnit() {
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.KB, FileSizeUnit.BYTE)).isEqualTo(1024L);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.MB, FileSizeUnit.BYTE)).isEqualTo(1024L * 1024);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.GB, FileSizeUnit.BYTE))
        .isEqualTo(1024L * 1024 * 1024);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.TB, FileSizeUnit.BYTE))
        .isEqualTo(1024L * 1024 * 1024 * 1024);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.PB, FileSizeUnit.BYTE))
        .isEqualTo(1024L * 1024 * 1024 * 1024 * 1024);

    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.MB, FileSizeUnit.KB)).isEqualTo(1024L);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.GB, FileSizeUnit.KB)).isEqualTo(1024L * 1024);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.TB, FileSizeUnit.KB))
        .isEqualTo(1024L * 1024 * 1024);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.PB, FileSizeUnit.KB))
        .isEqualTo(1024L * 1024 * 1024 * 1024);

    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.GB, FileSizeUnit.MB)).isEqualTo(1024L);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.TB, FileSizeUnit.MB)).isEqualTo(1024L * 1024);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.PB, FileSizeUnit.MB))
        .isEqualTo(1024L * 1024 * 1024);

    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.TB, FileSizeUnit.GB)).isEqualTo(1024L);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.PB, FileSizeUnit.GB)).isEqualTo(1024L * 1024);

    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.PB, FileSizeUnit.TB)).isEqualTo(1024L);
  }

  @Test
  void testConvertToHigherUnit() {
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.BYTE, FileSizeUnit.KB)).isEqualTo(0L);
    assertThat(FileSizeUnit.toUnit(1L, FileSizeUnit.GB, FileSizeUnit.PB)).isEqualTo(0L);
    assertThat(FileSizeUnit.toUnit(1500L, FileSizeUnit.GB, FileSizeUnit.TB)).isEqualTo(1L);
    assertThat(FileSizeUnit.toUnit(1600L, FileSizeUnit.BYTE, FileSizeUnit.KB)).isEqualTo(2L);
  }
}
