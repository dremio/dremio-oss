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

import static com.google.common.base.Predicates.alwaysFalse;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;

/** Extension class to force throw access denied errors on specific paths */
@NotThreadSafe
public class TestExtendedS3AFilesystem extends S3AFileSystem {

  private static Predicate<String> ACCESS_DENIED_PATH_CRITERIA = alwaysFalse();
  private static Predicate<String> GENERIC_ERROR_PATH_CRITERIA = alwaysFalse();

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throwIfNecessary("open", f);
    return super.open(f, bufferSize);
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    throwIfNecessary("getFileStatus", f);
    return super.getFileStatus(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    throwIfNecessary("list", f);
    return super.listFiles(f, recursive);
  }

  public static void setAccessDeniedExceptionOnPaths(Predicate<String> criteria) {
    ACCESS_DENIED_PATH_CRITERIA = criteria;
  }

  public static void noAccessDeniedExceptions() {
    ACCESS_DENIED_PATH_CRITERIA = alwaysFalse();
  }

  public static void setGenericExceptionOnPaths(Predicate<String> criteria) {
    GENERIC_ERROR_PATH_CRITERIA = criteria;
  }

  public static void noGenericExceptions() {
    GENERIC_ERROR_PATH_CRITERIA = alwaysFalse();
  }

  private void throwIfNecessary(String op, Path f) throws IOException {
    if (ACCESS_DENIED_PATH_CRITERIA.test(f.toString())) {
      throwAccessDeniedException(op, f.toString());
    } else if (GENERIC_ERROR_PATH_CRITERIA.test(f.toString())) {
      throwGenericException(op, f.toString());
    }
  }

  private void throwGenericException(String op, String path) throws IOException {
    AmazonS3Exception ex = new AmazonS3Exception("Internal");
    ex.setStatusCode(500);
    ex.setServiceName("Amazon S3");
    ex.setRequestId(UUID.randomUUID().toString());
    ex.setExtendedRequestId(UUID.randomUUID().toString());
    throw S3AUtils.translateException(op, path, ex);
  }

  private void throwAccessDeniedException(String op, String path) throws IOException {
    AmazonS3Exception ex = new AmazonS3Exception("Access Denied");
    ex.setStatusCode(403);
    ex.setErrorCode("Access Denied");
    ex.setServiceName("Amazon S3");
    ex.setRequestId(UUID.randomUUID().toString());
    ex.setExtendedRequestId(UUID.randomUUID().toString());
    throw S3AUtils.translateException(op, path, ex);
  }
}
