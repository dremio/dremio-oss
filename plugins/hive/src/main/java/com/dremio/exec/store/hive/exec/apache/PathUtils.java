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
package com.dremio.exec.store.hive.exec.apache;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.text.StrTokenizer;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.github.slugify.Slugify;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Utils to convert dotted path to file system path and vice versa.
 */
public class PathUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PathUtils.class);

  private static final char PATH_DELIMITER = '.'; // dot separated path
  private static final Joiner KEY_JOINER = Joiner.on(PATH_DELIMITER).useForNull("");
  private static final String SLASH = Path.SEPARATOR;
  private static final char SLASH_CHAR = Path.SEPARATOR_CHAR;
  private static final Joiner PATH_JOINER = Joiner.on(SLASH_CHAR).useForNull("");
//  private static final Path ROOT_PATH = Path.of(SLASH);
  private static final List<String> EMPTY_SCHEMA_PATHS = Collections.emptyList();

  /**
   * Convert list of components into fs path.
   * [a,b,c] -> /a/b/c
   * @param schemaPath list of path components
   * @return {@code fs Path}
   */
//  public static Path toFSPath(final List<String> schemaPath) {
//    if (schemaPath == null || schemaPath.isEmpty()) {
//      return ROOT_PATH;
//    }
//    return Path.of(ROOT_PATH, PATH_JOINER.join(schemaPath));
//  }

//  public static String toFSPathString(final List<String> schemaPath) {
//    return toFSPath(schemaPath).toURI().getPath();
//  }

  /**
   * Convert list of path components into fs path, skip root of path if its same as given root.
   * schema path : [a,b,c] and root: a -> /b/c
   * @param schemaPath list of path components
   * @param root of path to be ignored.
   * @return {@code fs Path}
   */
//  public static Path toFSPathSkipRoot(final List<String> schemaPath, final String root) {
//    if (schemaPath == null || schemaPath.isEmpty()) {
//      return ROOT_PATH;
//    }
//    if (root == null || !root.equals(schemaPath.get(0))) {
//      return toFSPath(schemaPath);
//    } else {
//      return toFSPath(schemaPath.subList(1, schemaPath.size()));
//    }
//  }

  /**
   * Convert fully or partially dotted path to file system path.
   * a.b.c   -> /a/b/c
   * a.`b/c` -> /a/b/c
   * @param path a fully or partially dotted path
   * @return {@code fs Path}
   */
//  public static Path toFSPath(String path) {
//    final List<String> pathComponents = Lists.newArrayList();
//    for (String component: parseFullPath(path)) {
//      if (component.contains(SLASH)) {
//        pathComponents.addAll(toPathComponents(Path.of(component)));
//      } else {
//        pathComponents.add(component);
//      }
//    }
//    return toFSPath(pathComponents);
//  }

  /**
   * Get the name of the file or folder at Path
   * @param path
   * @return
   */
  public static String getQuotedFileName(Path path) {
    List<String> components = toPathComponents(path);
    return String.format("%1$s%2$s%1$s", SqlUtils.QUOTE, components.get(components.size() - 1));
  }

  /**
   * Convert fs path to dotted schema path.
   * /a/b/c -> a.b.c
   * @param fsPath filesystem path
   * @return schema path.
   */
  public static String toDottedPath(Path fsPath) {
    return constructFullPath(toPathComponents(fsPath));
  }

  /**
   * Convert fs path relative to parent to dotted schema path.
   * parent: /a/b, child: /a/b/c/d -> c.d
   * @param parent parent path
   * @param child full path of child inside parent path
   * @return dotted schema name of child relative to parent.
   * @throws IOException if child does not belong under parent
   */
  public static String toDottedPath(final Path parent, final Path child) throws IOException {
    final List<String> parentPathComponents = toPathComponents(parent);
    final List<String> childPathComponents = toPathComponents(child);
    for (int i = 0; i < parentPathComponents.size(); ++i) {
      if (!parentPathComponents.get(i).equals(childPathComponents.get(i))) {
        throw new IOException(String.format("Invalid file/directory %s listed under %s", child, parent));
      }
    }
    return constructFullPath(childPathComponents.subList(parentPathComponents.size(), childPathComponents.size()));
  }

  /**
   * Convert fs path to list of strings.
   * /a/b/c -> [a,b,c]
   * @param fsPath a string
   * @return list of path components
   */
  public static List<String> toPathComponents(String fsPath) {
    if (fsPath == null ) {
      return EMPTY_SCHEMA_PATHS;
    }

    final StrTokenizer tokenizer = new StrTokenizer(fsPath, SLASH_CHAR, SqlUtils.QUOTE).setIgnoreEmptyTokens(true);
    return tokenizer.getTokenList();
  }

  /**
   * Convert fs path to list of strings.
   * /a/b/c -> [a,b,c]
   * @param fsPath filesystem path
   * @return list of path components
   */
  public static List<String> toPathComponents(Path fsPath) {
    if (fsPath == null ) {
      return EMPTY_SCHEMA_PATHS;
    }

    return toPathComponents(fsPath.toUri().getPath());
  }

  /**
   * puts back ticks around components if they look like reserved keywords and joins them with .
   * @param pathComponents can not contain nulls
   * @return a dot delimited path
   * Convert a list of path components to fully qualified dotted schema path
   * [a,b,c]      -> a.b.c
   * [a,b,c-1]    -> a.b.`c-1`
   * [a,b,c.json] -> a.b.`c.json`
   */
  public static String constructFullPath(Collection<String> pathComponents) {
    final List<String> quotedPathComponents = Lists.newArrayList();
    for (final String component : pathComponents) {
      checkNotNull(component);
      quotedPathComponents.add(SqlUtils.quoteIdentifier(component));
    }
    return KEY_JOINER.join(quotedPathComponents);
  }

  private static Pattern pattern1 = Pattern.compile("\\%28");
  private static Pattern pattern2 = Pattern.compile("\\%29");
  private static Pattern pattern3 = Pattern.compile("\\+");
  private static Pattern pattern4 = Pattern.compile("\\%27");
  private static Pattern pattern5 = Pattern.compile("\\%21");
  private static Pattern pattern6 = Pattern.compile("\\%7E");

  /**
   * Encode URI component consistent with JavaScript
   * @param component
   * @return encoded string
   */
  public static String encodeURIComponent(String component) {
    final String encodedComponent = URLEncoder.encode(component, StandardCharsets.UTF_8);
    final String pattern1Str = pattern1.matcher(encodedComponent).replaceAll("(");
    final String pattern2Str = pattern2.matcher(pattern1Str).replaceAll(")");
    final String pattern3Str = pattern3.matcher(pattern2Str).replaceAll("%20");
    final String pattern4Str = pattern4.matcher(pattern3Str).replaceAll("'");
    final String pattern5Str = pattern5.matcher(pattern4Str).replaceAll("!");
    final String pattern6Str = pattern6.matcher(pattern5Str).replaceAll("~");
    return pattern6Str;
  }

  /**
   * Parase a fully qualified dotted schema path to list of strings.
   * a.b.`c.json` -> [a,b,c.json]
   * a.b.`c-1`    -> [a,b,c-1]
   * a.b.c        -> [a,b,c]
   * @param path dotted schema path
   * @return list of path components.
   */
  public static List<String> parseFullPath(final String path) {
    final StrTokenizer tokenizer = new StrTokenizer(path, PATH_DELIMITER, SqlUtils.QUOTE).setIgnoreEmptyTokens(true);
    return tokenizer.getTokenList();
  }

  public static String removeQuotes(String pathComponent) {
    if (pathComponent.charAt(0) == SqlUtils.QUOTE && pathComponent.charAt(pathComponent.length() - 1) == SqlUtils.QUOTE) {
      return pathComponent.substring(1, pathComponent.length() - 1);
    }
    return pathComponent;
  }

  public static Joiner getPathJoiner() {
    return PATH_JOINER;
  }

  public static char getPathDelimiter() {
    return PATH_DELIMITER;
  }

  public static Joiner getKeyJoiner() {
    return KEY_JOINER;
  }

  public static String slugify(Collection<String> pathComponents) {
    Slugify slg = new Slugify();
    return slg.slugify(constructFullPath(pathComponents));
  }

  public static String relativePath(Path absolutePath, Path basePath) {
    Preconditions.checkArgument(absolutePath.isAbsolute(), "absolutePath must be an absolute path");
    Preconditions.checkArgument(basePath.isAbsolute(), "basePath must be an absolute path");

    List<String> absolutePathComponents = Lists.newArrayList(toPathComponents(absolutePath)); // make a copy
    List<String> basePathComponents = toPathComponents(basePath);
    boolean hasCommonPrefix = basePathComponents.isEmpty(); // when basePath is "/", we always have a common prefix

    for (String base : basePathComponents) {
      if (absolutePathComponents.get(0).equals(base)) {
        absolutePathComponents.remove(0);
        hasCommonPrefix = true;
      } else {
        break;
      }
    }
    if (hasCommonPrefix) {
      return PATH_JOINER.join(absolutePathComponents);
    } else {
      return absolutePath.toString();
    }
  }

  /**
   * Make sure the <i>givenPath</i> refers to an entity under the given <i>basePath</i>. Idea is to avoid using ".." to
   * refer entities outside the ba
   * @param basePath
   * @param givenPath
   */
  public static void verifyNoAccessOutsideBase(Path basePath, Path givenPath) {
    final String givenPathNormalized = givenPath.toUri().getPath();
    final String basePathNormalized = basePath.toUri().getPath();

    if (!givenPathNormalized.startsWith(basePathNormalized)) {
      throw UserException.permissionError()
          .message("Not allowed to access files outside of the source root")
          .addContext("Source root", basePathNormalized)
          .addContext("Requested to path", givenPathNormalized)
          .build(logger);
    }
  }

  /**
   * Remove leading "/"s in the given path string.
   * @param path
   * @return
   */
  public static String removeLeadingSlash(String path) {
    if (path.length() > 0 && path.charAt(0) == '/') {
      String newPath = path.substring(1);
      return removeLeadingSlash(newPath);
    } else {
      return path;
    }
  }
}
