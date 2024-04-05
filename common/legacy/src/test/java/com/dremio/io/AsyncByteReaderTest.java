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
package com.dremio.io;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import java.io.FileNotFoundException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.Test;

public class AsyncByteReaderTest {
  @Test
  public void testVersionedReadFully_exceptionInReadFully() {
    AsyncByteReader byteReader =
        new ReusableAsyncByteReader() {
          @Override
          public CompletableFuture<Void> readFully(
              long offset, ByteBuf dst, int dstOffset, int len) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new RuntimeException("Something went wrong"));
            return completableFuture;
          }

          @Override
          public CompletableFuture<Void> checkVersion(String version) {
            return CompletableFuture.completedFuture(null);
          }
        };

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () -> byteReader.versionedReadFully("1", 0, mock(ByteBuf.class), 100, 100).join())
        .havingCause()
        .isInstanceOf(RuntimeException.class)
        .withMessage("Something went wrong");
  }

  @Test
  public void testVersionedReadFully_exceptionInCheckVersion() {
    AsyncByteReader byteReader =
        new ReusableAsyncByteReader() {
          @Override
          public CompletableFuture<Void> readFully(
              long offset, ByteBuf dst, int dstOffset, int len) {
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public CompletableFuture<Void> checkVersion(String version) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new FileNotFoundException("Not found"));
            return completableFuture;
          }
        };

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () -> byteReader.versionedReadFully("1", 0, mock(ByteBuf.class), 100, 100).join())
        .havingCause()
        .isInstanceOf(FileNotFoundException.class)
        .withMessage("Not found");
  }

  @Test
  public void testVersionedReadFully_multipleExceptions_oneFNFE() {
    AsyncByteReader byteReader =
        new ReusableAsyncByteReader() {
          @Override
          public CompletableFuture<Void> readFully(
              long offset, ByteBuf dst, int dstOffset, int len) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new RuntimeException("Something went wrong"));
            return completableFuture;
          }

          @Override
          public CompletableFuture<Void> checkVersion(String version) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new FileNotFoundException("Not found"));
            return completableFuture;
          }
        };

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () -> byteReader.versionedReadFully("1", 0, mock(ByteBuf.class), 100, 100).join())
        .havingCause()
        .isInstanceOf(FileNotFoundException.class)
        .withMessage("Not found");
  }

  @Test
  public void testVersionedReadFully_multipleExceptions_bothFNFE() {
    AsyncByteReader byteReader =
        new ReusableAsyncByteReader() {
          @Override
          public CompletableFuture<Void> readFully(
              long offset, ByteBuf dst, int dstOffset, int len) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new FileNotFoundException("Not found"));
            return completableFuture;
          }

          @Override
          public CompletableFuture<Void> checkVersion(String version) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new FileNotFoundException("Not found either"));
            return completableFuture;
          }
        };

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () -> byteReader.versionedReadFully("1", 0, mock(ByteBuf.class), 100, 100).join())
        .havingCause()
        .isInstanceOf(FileNotFoundException.class);
  }

  @Test
  public void testVersionedReadFully_multipleSameExceptions_noneFNFE() {
    AsyncByteReader byteReader =
        new ReusableAsyncByteReader() {
          @Override
          public CompletableFuture<Void> readFully(
              long offset, ByteBuf dst, int dstOffset, int len) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new RuntimeException("Something went wrong"));
            return completableFuture;
          }

          @Override
          public CompletableFuture<Void> checkVersion(String version) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new RuntimeException("Something went wrong"));
            return completableFuture;
          }
        };

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () -> byteReader.versionedReadFully("1", 0, mock(ByteBuf.class), 100, 100).join())
        .havingCause()
        .isInstanceOf(RuntimeException.class)
        .withMessage("Something went wrong");
  }

  @Test
  public void testVersionedReadFully_multipleDifferentExceptions_nonFNFE() {
    AsyncByteReader byteReader =
        new ReusableAsyncByteReader() {
          @Override
          public CompletableFuture<Void> readFully(
              long offset, ByteBuf dst, int dstOffset, int len) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(new RuntimeException("Something went wrong"));
            return completableFuture;
          }

          @Override
          public CompletableFuture<Void> checkVersion(String version) {
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(sleep());
            completableFuture.completeExceptionally(
                new IllegalArgumentException("Something went wrong here too"));
            return completableFuture;
          }
        };

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () -> byteReader.versionedReadFully("1", 0, mock(ByteBuf.class), 100, 100).join())
        .havingCause()
        .isInstanceOf(Exception.class);
  }

  @Test
  public void testVersionedReadFully_noExceptions() {
    AsyncByteReader byteReader =
        new ReusableAsyncByteReader() {
          @Override
          public CompletableFuture<Void> readFully(
              long offset, ByteBuf dst, int dstOffset, int len) {
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public CompletableFuture<Void> checkVersion(String version) {
            return CompletableFuture.completedFuture(null);
          }
        };

    byteReader.versionedReadFully("1", 0, mock(ByteBuf.class), 100, 100).join();
  }

  private Runnable sleep() {
    return () -> {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    };
  }
}
