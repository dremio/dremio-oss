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
package com.dremio.exec.store.excel;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.dremio.exec.store.easy.excel.xls.XlsInputStream;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.poi.util.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestXlsInputStream extends BaseTestQuery {

  private final List<ArrowBuf> buffers = Lists.newArrayList();

  private final XlsInputStream.BufferManager bufferManager =
      new XlsInputStream.BufferManager() {
        @Override
        public ArrowBuf allocate(int size) {
          final ArrowBuf buf = getAllocator().buffer(size);
          buffers.add(buf);
          return buf;
        }
      };

  @Before
  public void before() {
    buffers.clear();
  }

  @After
  public void after() {
    for (ArrowBuf buf : buffers) {
      buf.close();
    }
  }

  /**
   * Following test loads a 400KB xls file using both an XlsInputStream and into a byte array, then
   * assesses the XlsInputStream is returning the correct data.
   */
  @Test
  public void testXlsInputStream() throws Exception {
    final String filePath = TestTools.getWorkingPath() + "/src/test/resources/excel/poi44891.xls";

    final byte[] bytes = IOUtils.toByteArray(new FileInputStream(filePath));

    final XlsInputStream xlsStream =
        new XlsInputStream(bufferManager, new ByteArrayInputStream(bytes));

    final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    final int bufferSize = 500;
    final byte[] buf1 = new byte[bufferSize];
    final byte[] buf2 = new byte[bufferSize];

    int offset = 0;
    while (offset < bytes.length) {
      int r1 = bis.read(buf1);
      int r2 = xlsStream.read(buf2);

      Assert.assertEquals(String.format("Error after reading %d bytes", offset), r1, r2);

      final String errorMsg =
          String.format(
              "Error after reading %d bytes%n" + "expected: %s%n" + "was: %s%n",
              offset, Arrays.toString(buf1), Arrays.toString(buf2));
      Assert.assertTrue(errorMsg, Arrays.equals(buf1, buf2));

      offset += bufferSize;
    }
  }
}
