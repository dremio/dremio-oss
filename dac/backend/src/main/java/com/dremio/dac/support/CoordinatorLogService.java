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
package com.dremio.dac.support;

import static com.dremio.dac.support.SupportService.DREMIO_LOG_PATH_PROPERTY;
import static com.dremio.dac.support.SupportService.TEMPORARY_SUPPORT_PATH;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import javax.inject.Provider;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import com.dremio.dac.service.support.CoordinatorLogServiceGrpc;
import com.dremio.dac.service.support.SupportBundleRPC.Chunk;
import com.dremio.dac.service.support.SupportBundleRPC.LogRequest;
import com.dremio.exec.server.SabotContext;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;


/**
 * CoordinatorLogService provides the coordinator log file input stream for QueryLogBundleService.
 * Includes generic helper functions for QueryLogBundleService.
 */
public class CoordinatorLogService extends CoordinatorLogServiceGrpc.CoordinatorLogServiceImplBase {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordinatorLogService.class);
  private static final int BUFFER_SIZE = 4096;

  private final Provider<SabotContext> sabotContextProvider;
  private final Provider<SupportService> supportServiceProvider;

  public CoordinatorLogService(Provider<SabotContext> sabotContextProvider,
                               Provider<SupportService> supportServiceProvider) {
    this.sabotContextProvider = sabotContextProvider;
    this.supportServiceProvider = supportServiceProvider;
  }

  @Override
  public void getServerLog(LogRequest request, StreamObserver<Chunk> responseObserver) {
    logger.debug("start writing {} over grpc ", request.getType());
    final Path dremioLogPath = Paths.get(System.getProperty(DREMIO_LOG_PATH_PROPERTY, "/var/log/dremio"));
    final long start = request.getStart();
    final long end = request.getEnd();

    switch (request.getType()) {
      case SERVER_LOG:
        getServerLog(dremioLogPath, responseObserver, start, end);
        break;
      case QUERIES_JSON:
        getQueriesLog(dremioLogPath, responseObserver, start, end);
        break;
      case SERVER_GC:
        getGCLog(dremioLogPath, responseObserver, start, end);
        break;
      case SERVER_OUT:
        getServerOut(dremioLogPath, responseObserver);
        break;
      default:
        logger.error("Invalid log type {}", request.getType());
    }

    responseObserver.onCompleted();
  }

  private void getServerLog(Path dremioLogPath, StreamObserver<Chunk> responseObserver, long start, long end) {
    // obtain all needed server gzip log in archive/
    final File[] allArchiveServerLogFiles = dremioLogPath.resolve("archive").toFile().listFiles(
      (dir, name) -> name.startsWith("server.") && name.endsWith(".log.gz")
        && DateTimeUtils.isBetweenDay(name, start, end, DateTimeZone.UTC));

    // if there are server logs in archive/, sort them and send their raw stream to response
    if (allArchiveServerLogFiles != null) {
      QueryLogBundleService.sortLogFilesInTimeOrder(allArchiveServerLogFiles);
      for (File file : allArchiveServerLogFiles) {
        writeAFileToResponseObserver(file, responseObserver);
      }
    }
    // if query is run today, compress server.log first then send to response
    if (DateTimeUtils.isToday(end, DateTimeZone.UTC)) {
      compressAPlainTextToResponseObserver(dremioLogPath, "server.log", responseObserver);
    }
  }

  private void getQueriesLog(Path dremioLogPath, StreamObserver<Chunk> responseObserver, long start, long end) {
    // obtain all need query gzip log in archive/
    final File[] allArchiveQueriesLogFiles = dremioLogPath.resolve("archive").toFile().listFiles(
      (dir, name) -> name.startsWith("queries.") && name.endsWith(".json.gz")
        && DateTimeUtils.isBetweenDay(name, start, end, DateTimeZone.UTC));

    // if there are queries logs in archive/, sort them and write their raw stream to one zip entry
    if (allArchiveQueriesLogFiles != null) {
      QueryLogBundleService.sortLogFilesInTimeOrder(allArchiveQueriesLogFiles);
      for (File file : allArchiveQueriesLogFiles) {
        writeAFileToResponseObserver(file, responseObserver);
      }
    }
    // if query is run today, compress queries.json first then send to response
    if (DateTimeUtils.isToday(end, DateTimeZone.UTC)) {
      compressAPlainTextToResponseObserver(dremioLogPath, "queries.json", responseObserver);
    }

  }

  private void getGCLog(Path inputDir, StreamObserver<Chunk> responseObserver, long start, long end) {

    final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
    // write the entire server out to response
    final File[] allGcLogFiles = inputDir.toFile().listFiles((dir, name) -> name.startsWith("server.gc"));

    if (allGcLogFiles != null) {
      // sort all gc logs in time order based on filename
      Arrays.sort(allGcLogFiles, Collections.reverseOrder());
      // compress gc logs (filter by days) into one file, then send to response, finally delete it
      File gzipFile = tempSupportDir.resolve(UUID.randomUUID().toString() + ".gc.gz").toFile();
      try {
        filterFilesContentByDayAndCompress(allGcLogFiles, gzipFile, start, end);
        writeAFileToResponseObserver(gzipFile, responseObserver);
      } finally {
        gzipFile.delete();
      }

    }
  }

  private void getServerOut(Path dremioLogPath, StreamObserver<Chunk> responseObserver) {
    // write the entire server out to response
    compressAPlainTextToResponseObserver(dremioLogPath, "server.out", responseObserver);
  }

  /**
   * Scan a file and select lines for the day(s) that overlap with [startLong, endLong].
   * Write the content to gzipFile
   * @param files a list of files in order
   * @param gzipFile
   * @param startLong
   * @param endLong
   */
  static void filterFilesContentByDayAndCompress(File[] files,
                                                  File gzipFile,
                                                  long startLong, long endLong) {

    final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final LocalDate start = new DateTime(startLong).toLocalDate();
    final LocalDate end = new DateTime(endLong).toLocalDate();

    try (GZIPOutputStream gzos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(gzipFile)))) {
      boolean reachedLeftBound = false;
      boolean reachedRightBound = false;
      for (File file : files) {
        if (reachedRightBound) {
          break;
        }
        try (BufferedReader archiveGcLogReader = new BufferedReader(new FileReader(file))) {
          for (String line = archiveGcLogReader.readLine(); line != null; line = archiveGcLogReader.readLine()) {
            try {
              Date now = dateFormat.parse(line);
              DateTime nowDateTime = new DateTime(now.getTime());
              LocalDate nowLocalDate = nowDateTime.toLocalDate();
              if (!reachedLeftBound && !nowLocalDate.isBefore(start)) {
                reachedLeftBound = true;
              }
              if (endLong != 0  && nowLocalDate.isAfter(end)) {
                reachedRightBound = true;
                break;
              }
            } catch (ParseException skip) {
              // this line does not include datetime
            }
            if (reachedLeftBound) {
              try {
                gzos.write(("\n" + line).getBytes());
              } catch (IOException unexpected) {
                // couldn't write a line to gzip
                logger.error("Couldn't write to gzip. {}", unexpected.toString());
                reachedRightBound = true;
                break;
              }
            }
          }
        } catch (IOException e) {
          logger.warn("Couldn't close file. {} {}", file.getAbsolutePath(), e);
        }
      }
    } catch (FileNotFoundException e) {
      logger.error("File not found {}. {}", gzipFile.toPath(), e);
    } catch (IOException e) {
      logger.error("Failed to close {}. {}", gzipFile, e);
    }
  }

  static File compressAPlainText(Path plainFilePath, Path outDir, String outputFilename) {

    Path outputGzipPath = outDir.resolve(outputFilename);
    try (
      GZIPOutputStream gzos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputGzipPath.toFile())));
      BufferedInputStream fileStream = new BufferedInputStream(new FileInputStream(plainFilePath.toFile()))
    ) {
      // write plain text to a gzip file
      copyFromInputStreamToOutStream(fileStream, gzos);
    } catch (FileNotFoundException e) {
      logger.warn("File not found: {}. {}", plainFilePath, e);
    } catch (IOException e) {
      logger.error("Failed to compress {}. {}", plainFilePath, e);
    }

    return outputGzipPath.toFile();
  }

  private static void writeAFileToResponseObserver(File file, StreamObserver<Chunk> responseObserver) {
    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file))) {

      byte[] buf = new byte[BUFFER_SIZE];
      int len;
      while ((len = bufferedInputStream.read(buf)) > -1) {
        responseObserver.onNext(Chunk.newBuilder()
          .setContent(ByteString.copyFrom(buf))
          .setLength(len)
          .build());
      }

    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  private void compressAPlainTextToResponseObserver(Path inputDir, String inputFileName,
                                                    StreamObserver<Chunk> responseObserver) {

    final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
    final Path inputLogPath = inputDir.resolve(inputFileName);
    // compress the file at {inputDir}/{inputFileName} and save to {tempSupportDir}/{uuid}_{inputFileName}.gz
    File gzipFile = compressAPlainText(inputLogPath, tempSupportDir, UUID.randomUUID().toString() + "_" + inputFileName + ".gz");
    // Copy the compressed file to responseObserver, then delete the compressed file
    try {
      writeAFileToResponseObserver(gzipFile, responseObserver);
    } finally {
      gzipFile.delete();
    }

  }

  private static void copyFromInputStreamToOutStream(InputStream input, OutputStream output) throws IOException {
    byte[] buf = new byte[BUFFER_SIZE];
    int len;
    while ((len = input.read(buf)) > -1) {
      output.write(buf, 0, len);
    }
  }


}
