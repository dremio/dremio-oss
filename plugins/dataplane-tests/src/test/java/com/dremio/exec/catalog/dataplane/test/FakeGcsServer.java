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
package com.dremio.exec.catalog.dataplane.test;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.dremio.common.util.Retryer;
import com.dremio.common.util.Retryer.WaitStrategy;
import com.google.common.base.StandardSystemProperty;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

public abstract class FakeGcsServer implements AutoCloseable {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(FakeGcsServer.class);
  private static final String FORCE_TEST_CONTAINERS = "dremio.test-containers.force";
  private static final String FAKE_GCS_SERVER_VERSION = "1.49.1";
  private static final String DOWNLOAD_PREFIX =
      "https://github.com/fsouza/fake-gcs-server/releases/download";

  public static FakeGcsServer startServer() {
    Retryer retryer =
        Retryer.newBuilder()
            .retryIfExceptionOfType(FakeGcsServerStartException.class)
            .setWaitStrategy(WaitStrategy.FLAT, 50, 50)
            .setMaxRetries(5)
            .build();

    return retryer.call(
        () -> {
          // We spin up one storage server for each test class. They need different ports so that
          // they do not collide. Unfortunately, we cannot use port 0 because fake-gcs-server
          // reports its own internal port in the "location" property of HTTP responses.
          FakeGcsServer server = newServer(findPort());
          server.start();
          return server;
        });
  }

  private static FakeGcsServer newServer(int httpPort) {
    if (Boolean.getBoolean(FORCE_TEST_CONTAINERS)) {
      return new TestContainersFakeGcsServer(httpPort);
    } else {
      return new NativeFakeGcsServer(httpPort);
    }
  }

  abstract void start();

  abstract int getHttpPort();

  private static class TestContainersFakeGcsServer extends FakeGcsServer {

    private final FakeGcsServerContainer container;

    public TestContainersFakeGcsServer(int httpPort) {
      this.container = new FakeGcsServerContainer(httpPort);
    }

    @Override
    public void start() {
      container.start();
    }

    @Override
    public void close() {
      container.close();
    }

    @Override
    public int getHttpPort() {
      return container.getHttpPort();
    }
  }

  private static class NativeFakeGcsServer extends FakeGcsServer {

    private final int httpPort;
    private final Path temporaryDirectory;

    private Path fakeGcsServerBinary;
    private Process fakeGcsServerProcess;

    public NativeFakeGcsServer(int httpPort) {
      this.httpPort = httpPort;

      try {
        this.temporaryDirectory = Files.createTempDirectory("fake-gcs-server-");
      } catch (IOException e) {
        throw new RuntimeException("Failed to setup fake-gcs-server temporary directory.", e);
      }
      temporaryDirectory.toFile().deleteOnExit();
    }

    @Override
    void start() {
      final String os =
          Objects.requireNonNull(StandardSystemProperty.OS_NAME.value()).toLowerCase(Locale.ROOT);
      final String arch =
          Objects.requireNonNull(StandardSystemProperty.OS_ARCH.value()).toLowerCase(Locale.ROOT);

      final String platform;
      final HashCode checksum;
      if (os.contains("mac")) {
        if ("aarch64".equals(arch)) {
          platform = "Darwin_arm64";
          checksum =
              HashCode.fromString(
                  "8445abfa29af99ba506d4f4880ae530b56a515e8765d8b7ff3bc0ad2d724a288");
        } else {
          platform = "Darwin_amd64";
          checksum =
              HashCode.fromString(
                  "ea96a1d866d8100c72b3a52366b86692f9d6f1ab3f687c85c8bda1f12632af20");
        }
      } else if (os.contains("linux") || os.contains("unix")) {
        if ("aarch64".equals(arch)) {
          platform = "Linux_arm64";
          checksum =
              HashCode.fromString(
                  "509eae8c89dd36b98b7fc7587dae04a6d4c2b78f733814d1cf1c496cfe6b0885");
        } else {
          platform = "Linux_amd64";
          checksum =
              HashCode.fromString(
                  "fc8b64cc0886833b779e891159d108715fcb0fb679f7f52abbeb7c65b53f2f88");
        }
      } else {
        throw new UnsupportedOperationException(
            "Unable to run fake-gcs-server on unsupported operating system.");
      }

      if (fakeGcsServerBinary == null) {
        try {
          fakeGcsServerBinary = fetchNativeBinary(platform, checksum);
        } catch (IOException e) {
          throw new RuntimeException("Failed to fetch native fake-gcs-server binary", e);
        }
      }

      try {
        fakeGcsServerProcess =
            new ProcessBuilder(
                    fakeGcsServerBinary.toString(),
                    "-scheme",
                    "http",
                    "-host",
                    "127.0.0.1",
                    "-port",
                    String.valueOf(httpPort),
                    "-backend",
                    "memory")
                .inheritIO()
                .redirectErrorStream(true)
                .start();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Most likely, the port was already in use
      if (!fakeGcsServerProcess.isAlive()) {
        throw new FakeGcsServerStartException();
      }
    }

    @Override
    public void close() {
      if (fakeGcsServerProcess != null) {
        fakeGcsServerProcess.destroy();
      }
    }

    @Override
    int getHttpPort() {
      return httpPort;
    }

    private Path fetchNativeBinary(String platform, HashCode checksum) throws IOException {
      final String archiveFilename =
          String.format("fake-gcs-server_%s_%s.tar.gz", FAKE_GCS_SERVER_VERSION, platform);
      final URL downloadUrl =
          new URL(DOWNLOAD_PREFIX + "/v" + FAKE_GCS_SERVER_VERSION + "/" + archiveFilename);

      final Path sharedTempDirectory =
          Paths.get(
              Objects.requireNonNull(StandardSystemProperty.USER_HOME.value()),
              ".cache/fake-gcs-server/");
      Files.createDirectories(sharedTempDirectory);

      final Path emulatorCacheArchive = sharedTempDirectory.resolve(archiveFilename);
      boolean shouldDownload = true;

      // If the emulator cache file exists, verify it has the correct hash.
      if (Files.exists(emulatorCacheArchive)) {
        try (final InputStream inputStream = Files.newInputStream(emulatorCacheArchive)) {
          copyWithVerification(inputStream, NullOutputStream.NULL_OUTPUT_STREAM, checksum);
          shouldDownload = false;
        } catch (IOException e) {
          LOGGER.error("Failed to validate the cached fake-gcs-server archive.", e);
        }
      }

      if (shouldDownload) {
        final Path temporaryEmulatorFile =
            Files.createTempFile(sharedTempDirectory, "emulator-", null);

        try (final InputStream downloadInputStream = downloadUrl.openStream();
            final OutputStream fileOutputStream = Files.newOutputStream(temporaryEmulatorFile)) {
          copyWithVerification(downloadInputStream, fileOutputStream, checksum);

          // Move the file into the cache, replacing any existing file.
          Files.move(temporaryEmulatorFile, emulatorCacheArchive, REPLACE_EXISTING, ATOMIC_MOVE);
        } catch (IOException exception) {
          try {
            Files.delete(temporaryEmulatorFile);
          } catch (IOException e) {
            LOGGER.warn("Failed to clean up temporary fake-gcs-server emulator archive file.", e);
            exception.addSuppressed(e);
          }

          LOGGER.error(
              "Failed to validate the downloaded fake-gcs-server emulator archive.", exception);
          throw exception;
        }
      }

      // In this process, we expect there to only ever be a flat file structure within the tar.gz,
      // in all other cases we error out.
      try (final TarArchiveInputStream tar =
          new TarArchiveInputStream(
              new GzipCompressorInputStream(Files.newInputStream(emulatorCacheArchive)))) {
        TarArchiveEntry tarEntry;
        while ((tarEntry = tar.getNextTarEntry()) != null) {
          final Path destination = temporaryDirectory.resolve(tarEntry.getName());
          destination.toFile().deleteOnExit();

          if (tarEntry.isFile()) {
            Files.copy(tar, destination);
            Files.setPosixFilePermissions(
                destination,
                EnumSet.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE));
          } else {
            throw new UnsupportedOperationException(
                String.format("Failed to process path %s, unexpected type.", destination));
          }
        }
      }

      final Path emulatorMain = temporaryDirectory.resolve("fake-gcs-server");
      if (!Files.exists(emulatorMain)) {
        throw new IOException("Unable to locate fake-gcs-server file.");
      }

      return emulatorMain;
    }

    private void copyWithVerification(
        InputStream inputStream, OutputStream outputStream, HashCode checksum) throws IOException {
      try (final HashingInputStream hashingInputStream =
          new HashingInputStream(Hashing.sha256(), inputStream)) {
        IOUtils.copy(hashingInputStream, outputStream);

        final HashCode sha256Checksum = hashingInputStream.hash();
        if (!sha256Checksum.equals(checksum)) {
          throw new IOException(
              String.format(
                  "Failed to validate the file, encountered checksum %s, expected %s.",
                  sha256Checksum, checksum));
        }
      }
    }
  }

  private static final class FakeGcsServerStartException extends RuntimeException {}

  private static int findPort() {
    Retryer retryer =
        Retryer.newBuilder()
            .retryIfExceptionOfType(IOException.class)
            .setWaitStrategy(WaitStrategy.FLAT, 10, 10)
            .setMaxRetries(5)
            .build();

    return retryer.call(
        () -> {
          ServerSocket serverSocket = new ServerSocket(0);
          int port = serverSocket.getLocalPort();
          serverSocket.close();
          return port;
        });
  }
}
