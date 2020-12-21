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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.NotSupportedException;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.provision.service.ProvisioningHandlingException;
import com.dremio.service.Service;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.users.UserNotFoundException;


/**
 * Service responsible for generating query log bundle
 * when a user asks for support help.
 */
@Options
public interface QueryLogBundleService extends Service {

  org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryLogBundleService.class);
  TypeValidators.BooleanValidator USERS_BUNDLE_DOWNLOAD = new TypeValidators.BooleanValidator("support.users.bundle.download", false);

  DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");

  String SYSTEMINFO_LOG_PATH = "system_info.json.gz";
  String COOR_LOG_PATH = "server.log.gz";
  String QUERY_LOG_PATH = "queries.json.gz";
  String GC_LOG_PATH = "server.gc.gz";
  String SERVER_OUT_LOG_PATH = "server.out.gz";
  int BUFFER_SIZE = 4096;

  /**
   * Get all the logs relevant to the query in the cluster
   * @param jobId job id in String
   * @param userName
   * @return PipedOutputStream
   * @throws UserNotFoundException if user name is not found
   * @throws JobNotFoundException if job is not found when requests the job profile
   * @throws IOException if fail to prepare piped stream
   * @throws ProvisioningHandlingException if error in getting yarn cluster info
   * @throws NotSupportedException if the current user does not have permission to download bundle
   * @throws RuntimeException if anther download request is triggered while one is happening
   */
  InputStream getClusterLog(String jobId, String userName)
    throws UserNotFoundException, JobNotFoundException, IOException, ProvisioningHandlingException, NotSupportedException;

  /**
   * Validate user role and support access control
   * @param userName
   * @throws UserNotFoundException if user name is not found
   * @throws NotSupportedException if the user doesn't have permission to download bundle
   */
  void validateUser(String userName) throws UserNotFoundException, NotSupportedException;

  /**
   * Write a file to TarArchiveOutputStream with a buffer of 4096 bytes
   *
   * @param taros      output
   * @param file       input
   * @param entryName  name of the entry in tar
   * @param cleanUp    true if file deletion is needed after write to tar
   */
  static void writeAFileToTar(TarArchiveOutputStream taros,
                             File file, String entryName, boolean cleanUp) {
    if (file == null) {
      return;
    }
    logger.debug("Writing {} to tar entry {}", file.getAbsolutePath(), entryName);
    try (
      BufferedInputStream fileStream = new BufferedInputStream(new FileInputStream(file))
    ) {
      try {
        TarArchiveEntry entry = new TarArchiveEntry(file, entryName);
        taros.putArchiveEntry(entry);

        byte[] buf = new byte[BUFFER_SIZE];
        int len;
        while ((len = fileStream.read(buf)) > -1) {
          taros.write(buf, 0, len);
        }
      } catch (IOException e) {
        logger.warn("Failed to write file {} to tar. {}", file.getName(), e);
      } finally {
        taros.closeArchiveEntry();
        if (cleanUp) {
          file.delete();
        }
      }
    } catch (Exception e) {
      logger.warn("I/O error when read file {}. {}", file.getName(), e);
    }
  }

  /**
   * Sort files by filename in time order: date asc then index desc.
   * @param files a list of files that is to be sorted in place
   */
  static void sortLogFilesInTimeOrder(File[] files) {
    if (files == null || files.length == 0) {
      return;
    }
    Arrays.sort(files);
    int lastDate = -1;
    List<File> temp = new ArrayList<>();
    int k = 0;
    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      try {
        int fileDate = DateTimeUtils.getDateFromString(file.getName()).getDate();
        if (lastDate != -1 && lastDate != fileDate) {
          File[] sortedTemp = temp.toArray(new File[0]);
          Arrays.sort(sortedTemp, Collections.reverseOrder());
          for (File tempFile : sortedTemp) {
            files[k] = tempFile;
            k++;
          }
          temp = new ArrayList<>();
        }
        temp.add(file);
        lastDate = fileDate;
      } catch (Exception ignored) {
      }
    }
    if (temp.size() > 1) {
      File[] sortedTemp = temp.toArray(new File[0]);
      Arrays.sort(sortedTemp, Collections.reverseOrder());
      for (File tempFile : sortedTemp) {
        files[k] = tempFile;
        k++;
      }
    }
  }

}
