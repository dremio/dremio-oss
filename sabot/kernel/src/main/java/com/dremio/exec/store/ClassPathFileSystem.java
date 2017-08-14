/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.google.common.io.Resources;


public class ClassPathFileSystem extends FileSystem{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassPathFileSystem.class);

  static final String ERROR_MSG = "ClassPathFileSystem is read only.";

  public static final String SCHEME = "classpath";

  private Path working;

  @Override
  public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3, short arg4, long arg5,
      Progressable arg6) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public boolean delete(Path arg0) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public boolean delete(Path arg0, boolean arg1) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  private String getFileName(Path path){
    String file = path.toUri().getPath();
    if(file.charAt(0) == '/'){
      file = file.substring(1);
    }
    return file;
  }

  @Override
  public FileStatus getFileStatus(Path arg0) throws IOException {
    String file = getFileName(arg0);
    URL url;

    String directory = (file.endsWith("/")) ? file : file + "/";
    try {
      // try directory first because URLClassLoader will return an URL
      // but without the trailing slash needed by JarInputStream to decode it
      // properly
      try {
        url = Resources.getResource(directory);
      } catch(IllegalArgumentException e) {
        // Fallback trying for a file
        url = Resources.getResource(file);
      }
    } catch(IllegalArgumentException e) {
      throw new FileNotFoundException(String.format("Unable to find path %s.", arg0.toString()));
    }

    // Check if the resource is a local file
    FileSystem fs = FileSystem.getLocal(getConf());
    if (fs.getScheme().equals(url.getProtocol())) {
      try {
        FileStatus status = fs.getFileStatus(new Path(url.toURI()));
        status.setPath(arg0);
        return status;
      } catch (URISyntaxException e) {
        throw new IOException(String.format("Cannot convert path %s to uri", url), e);
      }
    }

    // Special handling for directories
    if (url.getPath().endsWith("/")) {
      return new FileStatus(0, true, 0, 0, System.currentTimeMillis(), arg0);
    }
    return new FileStatus(Resources.asByteSource(url).size(), false, 1, 8096, System.currentTimeMillis(), arg0);
  }

  @Override
  public URI getUri() {
    try {
      return new URI(SCHEME+":///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return working;
  }

  @Override
  public FileStatus[] listStatus(Path arg0) throws IOException {
    return new FileStatus[0];
  }

  @Override
  public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public FSDataInputStream open(Path arg0, int arg1) throws IOException {
    String file = getFileName(arg0);
    URL url = Resources.getResource(file);
    if(url == null){
      throw new IOException(String.format("Unable to find path %s.", arg0.getName()));
    }
    ResourceInputStream ris = new ResourceInputStream(Resources.toByteArray(url));
    return new FSDataInputStream(ris);
  }

  @Override
  public boolean rename(Path arg0, Path arg1) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public void setWorkingDirectory(Path arg0) {
    this.working = arg0;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  public static void main(String[] args) throws Exception{
    URI uri = new URI("classpath:///");
  }
}
