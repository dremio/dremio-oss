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

  /**
   * Constructs a CoordinatorLogService object.
   *
   * @param sabotContextProvider   the object that provides instances of SabotContext
   * @param supportServiceProvider the object that provides instances of SabotContext
   */
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

  /**
   * Gets server logs from the Dremio files on logs directory filtering by left and right bounds date.
   *
   * <p> Gets all the server gzip log filtering by the date. Then, sort the files by time and write each file
   * to response observer.
   *
   * @param dremioLogPath    the Dremio logs directory path
   * @param responseObserver the observable log of Dremio files
   * @param start            the left bound date in milliseconds
   * @param end              the right bound date in milliseconds
   */
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

  /**
   * Gets queries logs from the Dremio files on logs directory filtering by left and right bounds date.
   *
   * <p> Gets all the queries gzip log filtering by the date. Then, sort the files by time and write each file
   * to response observer. If  query is run today, compress server logs first then send to response.
   *
   * @param dremioLogPath    the Dremio logs directory path
   * @param responseObserver the observable log of Dremio files
   * @param start            the left bound date in milliseconds
   * @param end              the right bound date in milliseconds
   */
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

  /**
   * Gets Garbage Collector logs from the Dremio files on logs directory filtering by left and right bounds date and compress.
   *
   * @param inputDir         the Dremio logs directory path
   * @param responseObserver the observable log of Dremio files
   * @param start            the left bound date in milliseconds
   * @param end              the right bound date in milliseconds
   */
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

  /**
   * Compress and send the entire server out logs to response observer.
   *
   * @param dremioLogPath    the Dremio logs directory path
   * @param responseObserver the observable log of Dremio files
   */
  private void getServerOut(Path dremioLogPath, StreamObserver<Chunk> responseObserver) {
    // write the entire server out to response
    compressAPlainTextToResponseObserver(dremioLogPath, "server.out", responseObserver);
  }

  /**
   * Selects lines of a file for the days that overlap with left bound and right bound dates interval. Write the content to gzipFile.
   *
   * @param files     a list of files
   * @param gzipFile  the gzip file to write the content
   * @param startLong the left bound date
   * @param endLong   the right bound date
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

  /**
   *  Compresses a file from a plain text file path to a gzip file.
   *
   * @param plainFilePath   the file path from the text to be compressed
   * @param outDir          the directory of the compressed file
   * @param outputFilename  the file name of the compressed file
   * @return                a gzip file compressed
   */
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

  /**
   * Writes a log file in a given observer.
   *
   * @param file              the file log to be written
   * @param responseObserver  the the observable log of Dremio files
   */
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

  /**
   * Compresses a file from a plain text file path to a given observer.
   *
   * @param inputDir         the text directory path to be compressed
   * @param inputFileName    the text file name to be compressed
   * @param responseObserver the observer to receive the compressed file
   */
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

  /**
   * Transfers from file InputStream object to OutputStream object.
   *
   * @param input  the input file to be transformed
   * @param output the OutputStream object
   * @throws IOException If any exceptions or errors occurs.
   */
  private static void copyFromInputStreamToOutStream(InputStream input, OutputStream output) throws IOException {
    byte[] buf = new byte[BUFFER_SIZE];
    int len;
    while ((len = input.read(buf)) > -1) {
      output.write(buf, 0, len);
    }
  }


}
