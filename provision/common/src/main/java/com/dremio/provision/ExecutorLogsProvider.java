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
package com.dremio.provision;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Supplier;

import com.dremio.provision.ExecutorLogsProvider.ExecutorLogMetadata;
import com.dremio.provision.service.ProvisioningHandlingException;

/**
 * Interface to provide implementations for different container log
 */
public interface ExecutorLogsProvider extends Supplier<ExecutorLogMetadata> {

  /**
   * Get clusterType API
   * @return {@link ClusterType}
   */
  ClusterType getType();

  /**
   * prepare the executor logs download and return the number of files and errors
   * @param cluster Cluster identification of the set of executors
   * @param executorsAddr addresses of executors that we will get log from
   * @param outputDir directory of the output file
   * @param startTime start time for logs
   * @param endTime end time for the logs
   * @return number of executor log files and errors
   * @throws ProvisioningHandlingException if failed to get info from Resource Manager
   */
  int prepareExecutorLogsDownload(Cluster cluster, Collection<String> executorsAddr,
                             Path outputDir, long startTime, long endTime) throws ProvisioningHandlingException;

  /**
   * Store info about requesting an executor log and
   * either a resulting File or an Exception with error message if fails.
   * Use the stored info to communicate with QueryLogBundleService and
   * help QueryLogBundleService generate metadata file.
   */
  final class ExecutorLogMetadata {
    private final Object logLocator;
    private final File outputFile;
    private final String errorMessage;
    private final Exception exception;

    public ExecutorLogMetadata(Object logLocator, File outputFile, String errorMessage, Exception exception) {
      this.logLocator = logLocator;
      if (outputFile != null) {
        this.outputFile = new File(outputFile.getAbsolutePath());
      } else {
        this.outputFile = null;
      }
      this.errorMessage = errorMessage;
      this.exception = exception; // exception is not modified outside of this constructor
    }

    public File getOutputFile() {
      return new File(outputFile.getAbsolutePath());
    }

    public Object getLogLocator() {
      return logLocator;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public Exception getException() {
      return exception;
    }

    public boolean hasError() {
      return errorMessage != null || exception != null;
    }
  }

}
