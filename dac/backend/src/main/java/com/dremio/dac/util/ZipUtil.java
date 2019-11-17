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
package com.dremio.dac.util;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Helper class to stream out files as chunk
 */
public class ZipUtil {
  private int capacity;
  private OutputStream fsout;
  private ZipOutputStream zout;
  private FileSystem fileSystem;
  private int position;
  private int chunkCount;
  private String basePath;

  public ZipUtil(int capacity, FileSystem fileSystem, String basePath) {
    this.capacity = capacity;
    this.fileSystem = fileSystem;
    this.basePath = basePath;
    position = 0;
    chunkCount = 0;
  }

  public void writeFile(String filename, byte[] profile) throws IOException {
    int length = profile.length;
    if (position == 0) {
      setupZip(getNextChunkName());
      writeEntry(filename, profile);
      if (position > capacity) {
        close();
      }
    } else {
      if ((position + length) > capacity) {
        close();
        setupZip(getNextChunkName());
      }
      writeEntry(filename, profile);
    }
  }

  public void close() {
    try {
      if (zout != null) {
        zout.close();
      }
    } catch (IOException ex) {}
    position = 0;
    chunkCount++;
  }

  private void writeEntry(String filename, byte[] data) throws IOException {
    zout.putNextEntry(new ZipEntry(filename));
    zout.write(data);
    zout.closeEntry();
    position += data.length;
  }

  private void setupZip(String filename) throws IOException {
    fsout = fileSystem.create(Path.of(filename+".zip"), true);
    zout = new ZipOutputStream(fsout);
  }

  private String getNextChunkName() {
    Calendar currentTime = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    StringBuilder sb = new StringBuilder(basePath).append(timeFormat.format(currentTime.getTime())).append("_").append(chunkCount);
    return sb.toString();
  }
}
