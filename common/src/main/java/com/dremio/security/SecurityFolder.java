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
package com.dremio.security;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.Set;

import com.dremio.config.DremioConfig;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/**
 * Helper class to manage a security folder to store confidential information
 */
public final class SecurityFolder {

  private static final String SECURITY_DIRECTORY = "security";

  private static final Set<PosixFilePermission> SECURITY_DIRECTORY_PERMISSIONS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE);

  private static final Set<PosixFilePermission> SECURITY_FILE_PERMISSIONS =
      EnumSet.of(OWNER_READ, OWNER_WRITE);

  private final Path securityDirectory;


  /**
   * Create a security folder instance based on the dremio configuration.
   *
   * If it doesn't already exist, the security directory will be created directly under the
   * local write path.
   *
   * @param config the Dremio config
   * @throws IOException if the security directory cannot be created
   * @throws GeneralSecurityException if the security directory permissions are invalid
   * @throws IOException if the directory cannot be created, or a file with the same name already exists
   */
  public static SecurityFolder of(DremioConfig config) throws GeneralSecurityException, IOException {
    final String localWritePath = config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING);
    final Path path = Paths.get(localWritePath, SECURITY_DIRECTORY);

    Files.createDirectories(path, PosixFilePermissions.asFileAttribute(SECURITY_DIRECTORY_PERMISSIONS));
    return new SecurityFolder(path);
  }

  private SecurityFolder(Path path) throws GeneralSecurityException, IOException {
    this.securityDirectory = checkDirectoryPermissions(path);
  }

  /**
   * Check if a file or a directory already exists under the security folder
   * @param filename the file name
   * @return true if the directory already exists
   */
  public boolean exists(String filename) {
    return Files.exists(securityDirectory.resolve(filename));
  }

  /**
   * Check if the security folder already exist
   *
   * @param config
   * @param filename
   * @return
   */
  public static boolean exists(DremioConfig config, String filename) {
    final String localWritePath = config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING);
    return Files.exists(Paths.get(localWritePath, SECURITY_DIRECTORY, filename));
  }

  /**
   * Resolve a filename under the security directory
   *
   * @param filename the filename
   * @return the resolved path
   */
  public Path resolve(String filename) {
    return securityDirectory.resolve(filename);
  }

  /**
   * Option when opening a file for write
   */
  public enum OpenOption {
    /**
     * Create the file if necessary and open it for write
     */
    CREATE_OR_WRITE,

    /**
     * Create the file and fail if the file already exists
     */
    CREATE_ONLY,

    /**
     * Open the file for write, do not create it if it already exists.
     */
    NO_CREATE
  }

  /**
   * Create an output stream for a file under the security directory
   *
   * The file content will be created if it doesn't exist and {@code create} is true, and its content will be truncated first.
   * The file wille only accessible to the current user.
   *
   * @param filename the name of the file to open for write
   * @param openOptions how the file should be open
   * @return an output stream
   * @throws NoSuchFileException if the file doesn't exist and {@code create} is false
   * @throws IOException if an I/O error occurs
   * @throws GeneralSecurityException if the file already exists but permissions are invalid
   */
  public OutputStream newSecureOutputStream(String filename, OpenOption openOption) throws GeneralSecurityException, IOException {
    final Path filePath = securityDirectory.resolve(filename);
    boolean exists = Files.exists(filePath);
    if (!exists && openOption == OpenOption.NO_CREATE) {
      throw new NoSuchFileException(filePath.toString());
    }

    if (!exists) {
      final WritableByteChannel byteChannel = Files.newByteChannel(filePath, ImmutableSet.of(CREATE_NEW, WRITE), PosixFilePermissions.asFileAttribute(SECURITY_FILE_PERMISSIONS));
      return Channels.newOutputStream(byteChannel);
    } else {
      checkFilePermissions(filePath);
      final java.nio.file.OpenOption[] options = openOption == OpenOption.CREATE_ONLY
          ? new java.nio.file.OpenOption[] { CREATE_NEW, WRITE, TRUNCATE_EXISTING }
          : new java.nio.file.OpenOption[] { WRITE, TRUNCATE_EXISTING };
      return Files.newOutputStream(filePath, options);
    }
  }

  /**
   * Create an input stream for a file under the security directory
   *
   * Before opening the file for read, permissions will check and an exception will be thrown
   * if the file is accessible to other people than the current user.
   *
   * @param filename the file to read
   * @return an input stream
   * @throws IOException if an I/O error occurs
   * @throws GeneralSecurityException if the file permissions are invalid
   */
  public InputStream newSecureInputStream(String filename) throws GeneralSecurityException, IOException {
    Path filePath = securityDirectory.resolve(filename);
    checkFilePermissions(filePath);
    return Files.newInputStream(filePath);
  }

  private static Path checkDirectoryPermissions(Path path) throws GeneralSecurityException, IOException {
    return checkPathPermissions(path, "Directory", SECURITY_DIRECTORY_PERMISSIONS);
  }

  private static Path checkFilePermissions(Path path) throws GeneralSecurityException, IOException {
    return checkPathPermissions(path, "File", SECURITY_FILE_PERMISSIONS);
  }

  private static Path checkPathPermissions(Path path, String fileType, Set<PosixFilePermission> permissionReference) throws GeneralSecurityException, IOException {
    Set<PosixFilePermission> permission = Files.getPosixFilePermissions(path);
    checkSecurity(permissionReference.equals(permission), "%s %s is not accessible to owner only", fileType, path);
    return path;
  }

  @FormatMethod
  private static void checkSecurity(boolean check, @FormatString String message, Object... args) throws GeneralSecurityException {
    if (!check) {
      throw new GeneralSecurityException(String.format(message, args));
    }

  }

}
