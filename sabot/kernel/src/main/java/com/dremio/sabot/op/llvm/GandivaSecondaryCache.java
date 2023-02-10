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
package com.dremio.sabot.op.llvm;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.gandiva.evaluator.JavaSecondaryCacheInterface;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.google.common.base.Stopwatch;

import io.netty.util.internal.PlatformDependent;

/**
 * Implementation of the secondary cache for gandiva.
 */
public class GandivaSecondaryCache implements JavaSecondaryCacheInterface {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GandivaSecondaryCache.class);

  private static GandivaSecondaryCache secondaryCacheInstance;

  private final Path path;
  private final String arrowVersion;
  private final FileSystem fs;
  private final ExecutorService pool;

  private GandivaSecondaryCache(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
    arrowVersion = DremioVersionInfo.getArrowVersion();
    pool = Executors.newCachedThreadPool(new NamedThreadFactory("secondary-cache-writer-"));
  }

  public static synchronized void createInstance(FileSystem fs, Path p) {
    if (secondaryCacheInstance == null) {
      secondaryCacheInstance = new GandivaSecondaryCache(fs, p);
    }
  }

  public static GandivaSecondaryCache getInstance() {
    return secondaryCacheInstance;
  }

  /**
   * Create and return the arrow buffer containing object code.
   *
   * @param addrKey the address of the key to lookup in the secondary cache.
   * @param sizeKey the size of key to lookup in the secondary cache.
   * @return address and size of the arrow buffer.
   */
  @Override
  public BufferResult get(long addrKey, long sizeKey) {
    BufferResult res = null;

    byte[] bytesKey = new byte[(int) sizeKey];
    PlatformDependent.copyMemory(addrKey, bytesKey, 0, sizeKey);
    String hashKey = sha256Hex(bytesKey);
    Path prefix = Path.mergePaths(path, Path.of(arrowVersion));
    Path p = Path.mergePaths(prefix, Path.of(hashKey));

    logger.debug("Getting object code for key {} from Secondary Cache.", p);
    try {
      if (fs.exists(p)) {
        FSInputStream stream = fs.open(p);
        byte[] bytes = new byte[stream.available()];
        int count = stream.read(bytes);
        stream.close();

        long addr = PlatformDependent.allocateMemory(count);
        PlatformDependent.copyMemory(bytes, 0, addr, count);
        res = new BufferResult(addr, count);
      }
    } catch (IOException e) {
      logger.warn("Failed to read from Path {}.", p, e);
    }

    if (res == null) {
      logger.debug("Key {} not found in the secondary cache.", p);
    }
    return res;
  }

  /**
   * Set the secondary cache with the buffer containing the object code.
   *
   * @param addrKey   memory address of the key buffer.
   * @param sizeKey   size of the key buffer.
   * @param addrValue memory address of the value buffer.
   * @param sizeValue size of the value buffer.
   */
  @Override
  public void set(long addrKey, long sizeKey, long addrValue, long sizeValue) {
    byte[] bytesKey = new byte[(int) sizeKey];
    PlatformDependent.copyMemory(addrKey, bytesKey, 0, sizeKey);
    String hashKey = sha256Hex(bytesKey);
    Path prefix = Path.mergePaths(path, Path.of(arrowVersion));
    Path p = Path.mergePaths(prefix, Path.of(hashKey));
    try {
      if (!fs.exists(p)) {
        logger.debug("Setting object code for key {} to Secondary Cache.", p);
        byte[] bytesValue = new byte[(int) sizeValue];
        PlatformDependent.copyMemory(addrValue, bytesValue, 0, sizeValue);
        FSOutputStream stream = fs.create(p);

        Runnable writer = new BackgroundServiceWriter(stream, bytesValue);
        pool.submit(writer);
      }
    } catch (IOException e) {
      logger.warn("Failed to write to Path {}.", p, e);
    }
  }

  @Override
  public void releaseBufferResult(long addr) {
    PlatformDependent.freeMemory(addr);
  }

  // class to enable a background service which writes the object code to the file system
  private class BackgroundServiceWriter implements Runnable {
    private final byte[] byteValue;
    private final FSOutputStream stream;

    public BackgroundServiceWriter(FSOutputStream stream, byte[] byteValue) {
      this.stream = stream;
      this.byteValue = byteValue;
    }

    @Override
    public void run() {
      try {
        Stopwatch writeTime = Stopwatch.createUnstarted();
        writeTime.start();
        stream.write(byteValue);
        writeTime.stop();
        stream.close();
        logger.debug("Background service took {} to write to FS.", writeTime.elapsed(TimeUnit.MILLISECONDS));
      } catch (IOException e) {
        logger.warn("Background service failed to write to FS.", e);
      }
    }
  }
}
