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
package com.dremio.io.file;

import static com.dremio.io.file.UriSchemes.ADL_SCHEME;
import static com.dremio.io.file.UriSchemes.FILE_SCHEME;
import static com.dremio.io.file.UriSchemes.GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.S3_SCHEME;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.util.Preconditions;

import com.google.common.collect.ImmutableSet;

/**
 * Filesystem path
 *
 * A path is equivalent to a URI with a optional schema and authority
 * and a path component. Other URI components might be ignored.
 *
 * Paths can be compared. The comparison is equivalent to their URI representation.
 *
 */
public final class Path implements Comparable<Path> {
  public static final String AZURE_AUTHORITY_SUFFIX = ".blob.core.windows.net";
  public static final String CONTAINER_SEPARATOR = "@";

  public static final Set<String> S3_FILE_SYSTEM = ImmutableSet.of("s3a", S3_SCHEME, "s3n");
  public static final Set<String> GCS_FILE_SYSTEM = ImmutableSet.of(GCS_SCHEME);
  public static final Set<String> AZURE_FILE_SYSTEM = ImmutableSet.of("wasbs", "wasb", "abfs", "abfss");
  public static final Set<String> ADLS_FILE_SYSTEM = ImmutableSet.of(ADL_SCHEME);


  public static final Set<Object> validSchemes = ImmutableSet.builder().addAll(S3_FILE_SYSTEM).
    addAll(GCS_FILE_SYSTEM).addAll(AZURE_FILE_SYSTEM).addAll(ADLS_FILE_SYSTEM)
    .addAll(Arrays.stream(UriSchemes.class.getFields()).map(x -> {
      try {
        return x.get(null);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        return "";
      }
    }).collect(Collectors.toList())).build();

  public static final String SEPARATOR = "/";
  public static final char SEPARATOR_CHAR = '/';

  private static final Path CURRENT_DIRECTORY = Path.of(".");

  private final URI uri;

  private Path(URI uri) {
    this.uri = Objects.requireNonNull(uri).normalize();
    Preconditions.checkArgument(this.uri.getPath() != null, "Path URI requires a path component");
  }

  /**
   * Creates a path instance from the provided URI
   *
   * @param uri a non null URI with a path component
   * @throws NullPointerException if uri is {@code null}
   * @return a path instance
   * @throws NullPointerException if uri is {@code null}
   * @throws IllegalArgumentException if uri is not a valid URI (as specified by {@code URI})
   */
  public static Path of(URI uri) {
    return new Path(uri);
  }

  /**
   * Creates a path instance from the provided {@code path} string
   *
   * The path should be using the same format as {@code Path#toString()}. Notably the path
   * component should not be encoded.
   *
   * @param path a URI-like path declaration.
   * @return a path instance
   * @throws NullPointerException if uri is {@code null}
   * @throws IllegalArgumentException if uri is not a valid URI (as specified by {@code URI})
   */
  public static Path of(String path) {
    Objects.requireNonNull(path);
    Preconditions.checkArgument(!path.isEmpty(), "path should be a non-empty string");

    return of(toURI(path));
  }

  /**
   * Merges {@code path1} and {@code path2} together
   *
   * The merged path URI is created from {@code path1} scheme and authority, and the concatenation of
   * {@code path1} and {@code path2} path components (irrespective of {@code path2} being an absolute path).
   *
   * @param path1 the first path to merge
   * @param path2 the second path to tmerge
   * @return the merged path
   * @throws NullPointerException if {@code path1} or {@code path2} is {@code null}
   */
  public static Path mergePaths(Path path1, Path path2) {
    final String path1Path = path1.uri.getPath();
    final String path2Path = path2.uri.getPath();

    if (path2Path.isEmpty()) {
      return path1;
    }

    final StringBuilder finalPath = new StringBuilder(path1Path.length() + path2Path.length() + 1);
    finalPath.append(path1Path);
    if (!path1Path.isEmpty() && path1Path.charAt(path1Path.length() - 1) != SEPARATOR_CHAR) {
      finalPath.append(SEPARATOR_CHAR);
    }
    if (path2Path.charAt(0) != SEPARATOR_CHAR) {
      finalPath.append(path2Path);
    } else {
      finalPath.append(path2Path.substring(1));
    }

    try {
      return of(new URI(path1.uri.getScheme(), path1.uri.getAuthority(), finalPath.toString(), null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException();
    }
  }


  /**
   * Creates a path instance with no schema or authority
   *
   * @param path the path
   * @return a path instance
   * @throws NullPointerException if {@code path} is null
   */
  public static Path withoutSchemeAndAuthority(Path path) {
    Objects.requireNonNull(path);
    final URI uri = path.toURI();
    if (uri.getScheme() == null && uri.getAuthority() == null) {
      return path;
    }

    try {
      return of(new URI(null, null, uri.getPath(), null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  /**
   * Gets the filename
   *
   * The filename is the last component of the URI path element
   * @return the file name
   */
  public String getName() {
    final String path =  uri.getPath();
    int index = path.lastIndexOf(SEPARATOR_CHAR);
    return path.substring(index + 1);
  }

  /**
   * Gets the path's parent
   *
   * The parent has the URI schema and authority as this object (if any) and its
   * path element is this object's path element except for the last component.
   *
   * If the URI path's element is the root element ("/") or the current
   * directory ("."), then this function returns {@code null}.
   *
   * @return the path's parent
   */
  public Path getParent() {
    final String path = uri.getPath();

    final int index = path.lastIndexOf(SEPARATOR_CHAR);
    if (index == -1) {
      return CURRENT_DIRECTORY;
    }
    if (index == 0) {
      if (path.length() == 1) {
        // Single slash character -> this is root
        return null;
      }
      return Path.of(SEPARATOR);
    }

    final String parent = path.substring(0, index);
    try {
      return new Path(new URI(uri.getScheme(), uri.getAuthority(), parent, null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Resolves the given path against this path
   *
   * Resolution works as follows:
   * <ul>
   * <li>if {@code that} is absolute, or its path component is absolute, returns
   * {@code path}</li>
   * <li>if {@code path} is empty, returns this path</li>
   * <li>otherwise append {@code path} path component to this path component
   * (separated by the path separator if necesseray), and creates a new
   * {@code Path} instance with the current schema and authorite, and the new
   * path component.</li>
   * </ul>
   *
   * Note that {@code that} scheme and authority components are ignored
   *
   * @param path the path to resolve against
   * @return the resolved path
   */
  public Path resolve(Path path) {
    final Path that = Objects.requireNonNull(path);
    // Absolute path or path with absolute path components are returned as-is
    if (that.uri.isAbsolute() || that.isAbsolute()) {
      return that;
    }

    // If path is empty, return this (trivial resolution)
    final String thatPath = that.uri.getPath();
    if (thatPath.isEmpty()) {
      return this;
    }

    final String thisPath = this.uri.getPath();
    final StringBuilder finalPath = new StringBuilder(thisPath.length() + thatPath.length() + 1);
    finalPath.append(thisPath);
    if (!thisPath.isEmpty() && thisPath.charAt(thisPath.length() - 1) != SEPARATOR_CHAR) {
      finalPath.append(SEPARATOR_CHAR);
    }
    finalPath.append(thatPath);

    try {
      return of(new URI(this.uri.getScheme(), this.uri.getAuthority(), finalPath.toString(), null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Resolves the given path against this path
   *
   * Behavior is same as <code>Path.resolve(Path.of(that))</code>}
   * @param path the path to resolve against
   * @return
   */
  public Path resolve(String path) {
    Objects.requireNonNull(path);
    return resolve(Path.of(path));
  }

  /**
   * Checks if the path is absolute
   *
   * The path is absolute if its URI path's element is absolute.
   * @return {@code true} if the path is absolute, {@code false} otherwise
   */
  public boolean isAbsolute() {
    return uri.getPath().startsWith(SEPARATOR);
  }


  /**
   * Gets the path's depth
   *
   * The path's depth is the number of components in the URI path element.
   *
   * @return the path's depth
   */
  public int depth() {
    final String path = uri.getPath();

    if (path.charAt(0) == SEPARATOR_CHAR && path.length() == 1) {
      return 0;
    }

    int depth = 0;
    for (int i = 0 ; i < path.length(); i++) {
      if (path.charAt(i) == SEPARATOR_CHAR) {
        depth++;
      }
    }
    return depth;
  }

  /**
   * Gets the URI representation of this path
   * @return a URI instance
   */
  public URI toURI() {
    return uri;
  }

  public Path relativize(Path that) {
    return Path.of(this.uri.relativize(that.uri));
  }

  /**
   * Gets the string representation of this path
   *
   * The representation is a URI-like string, which might not be a valid URI at all.
   * Notably, the path component is represented without any proper uri encoding.
   *
   * The string representation can be parsed again using {@code Path#of(String)}
   *
   * @return a string
   */
  @Override
  public String toString() {
    return toString(this);
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Path)) {
      return false;
    }
    Path that = (Path) obj;
    return this.uri.equals(that.uri);
  }

  @Override
  public int compareTo(Path that) {
    return this.uri.compareTo(that.uri);
  }

  /*
   * Creates a path from a Hadoop URI-like string
   *
   * Hadoop Path.toString() method does not url-encode path component and cannot be
   * parsed directly using {@code URI#create(String)}
   *
   * @param uri
   * @return a path instance
   * @deprecated this method should only be used for preserving compatibility when
   * data is persisted
   */
  public static URI toURI(String uri) {
    final int colonIndexOf = uri.indexOf(':');
    final int slashIndexOf = uri.indexOf('/');

    int start = 0;
    // Check for scheme
    String scheme;
    if (colonIndexOf >= 0 && (slashIndexOf == -1 || slashIndexOf > colonIndexOf)) {
      scheme = uri.substring(0, colonIndexOf);
      start = colonIndexOf + 1;
    } else {
      scheme = null;
    }

    String authority;
    // Check for authority
    if (uri.startsWith("//", start)) {
      int rootSlashIndexOf = uri.indexOf('/', start + 2);
      if (rootSlashIndexOf > -1) {
        authority = uri.substring(start + 2, rootSlashIndexOf);
        start = rootSlashIndexOf;
      } else {
        authority = uri.substring(start + 2);
        start = uri.length();
      }
    } else {
      authority = null;
    }

    if(scheme != null && !validSchemes.contains(scheme)) {
      try {
        URI uriPath = new URI(null, authority, "/" + uri, null, null);
        return URI.create("/").relativize(uriPath);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }

    // path does not contain a scheme. Assume for a non url-encoded path
    try {
      return new URI(scheme, authority, uri.substring(start), null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /*
   * Returns a Hadoop-like path string
   *
   * The string is a possibly non-valid URI where the path component is not
   * url-encoded
   *
   * @param path the path
   * @return the string representation
   * @deprecated this method should only be used for preserving compatibility when
   * data is persisted
   */
  public static String toString(Path path) {
    final StringBuilder sb = new StringBuilder();
    final URI uri = path.uri;
    if (uri.getScheme() != null) {
      sb.append(uri.getScheme()).append(":");
    }
    if (uri.getAuthority() != null) {
      sb.append("//").append(uri.getAuthority());
    }
    if (uri.getPath() != null) {
      sb.append(uri.getPath());
    }

    return sb.toString();
  }

  /*
  Current container file system accepts path in a format container_Name/PathToObject
  This function is to convert container file path to relative path that ContainerFile System under stands
   */
  public static String getContainerSpecificRelativePath(Path path) {
    URI pathUri = path.uri;
    if (pathUri.getScheme() == null) {
      return path.toString();
    }
    String scheme = pathUri.getScheme().toLowerCase(Locale.ROOT);
    if (S3_FILE_SYSTEM.contains(scheme)) {
      String authority = (pathUri.getAuthority() != null) ? pathUri.getAuthority() : "";
      return SEPARATOR + authority + pathUri.getPath();
    } else if (AZURE_FILE_SYSTEM.contains(scheme)) {
      return SEPARATOR + pathUri.getUserInfo() + pathUri.getPath();
    } else if (GCS_FILE_SYSTEM.contains(scheme)) {
      String authority = (pathUri.getAuthority() != null) ? pathUri.getAuthority() : "";
      return SEPARATOR + authority + pathUri.getPath();
    } else if (ADLS_FILE_SYSTEM.contains(scheme)) {
      return Path.withoutSchemeAndAuthority(path).toString();
    } else if (FILE_SCHEME.equals(scheme)) {
      return pathUri.getPath();
    } else {
      return path.toString();
    }
  }
}
