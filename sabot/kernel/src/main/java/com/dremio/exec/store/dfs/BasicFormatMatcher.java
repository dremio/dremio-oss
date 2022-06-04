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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import com.dremio.io.CompressionCodec;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

public class BasicFormatMatcher extends FormatMatcher {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicFormatMatcher.class);

  protected final FormatPlugin plugin;
  protected final boolean compressible;

  private final List<Pattern> patterns;
  private final MagicStringMatcher matcher;

  public BasicFormatMatcher(FormatPlugin plugin, List<Pattern> patterns, List<MagicString> magicStrings) {
    super();
    this.patterns = ImmutableList.copyOf(patterns);
    this.matcher = new MagicStringMatcher(magicStrings);
    this.plugin = plugin;
    this.compressible = false;
  }

  public BasicFormatMatcher(FormatPlugin plugin, List<String> extensions, boolean compressible) {
    List<Pattern> patterns = Lists.newArrayList();
    for (String extension : extensions) {
      patterns.add(Pattern.compile(".*\\." + extension));
    }
    this.patterns = patterns;
    this.matcher = new MagicStringMatcher(new ArrayList<MagicString>());
    this.plugin = plugin;
    this.compressible = compressible;
  }

  @Override
  public boolean matches(FileSystem fs, FileSelection selection, CompressionCodecFactory codecFactory) throws IOException {
    Optional<FileAttributes> firstFileO = selection.getFirstFile();
    if(!firstFileO.isPresent()) {
      return false;
    }
    return matches(fs, firstFileO.get(), codecFactory);
  }

  /*
   * Function returns true if the file extension matches the pattern
   */
  @Override
  public boolean matches(FileSystem fs, FileAttributes attributes, CompressionCodecFactory codecFactory) throws IOException {
  CompressionCodec codec = null;
    if (compressible) {
      codec = codecFactory.getCodec(attributes.getPath());
    }
    String fileName = attributes.getPath().toString();
    String fileNameHacked = null;
    if (codec != null) {
      fileNameHacked = fileName.substring(0, fileName.lastIndexOf('.'));
    }

    // Check for a matching pattern for compressed and uncompressed file name
    for (Pattern p : patterns) {
      if (p.matcher(fileName).matches()) {
        return true;
      }
      if (fileNameHacked != null  &&  p.matcher(fileNameHacked).matches()) {
        return true;
      }
    }

    if (matcher.matches(fs, attributes)) {
      return true;
    }
    return false;
  }

  @Override
  @JsonIgnore
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }


  private class MagicStringMatcher {

    private List<RangeMagics> ranges;

    public MagicStringMatcher(List<MagicString> magicStrings) {
      ranges = Lists.newArrayList();
      for(MagicString ms : magicStrings) {
        ranges.add(new RangeMagics(ms));
      }
    }

    public boolean matches(FileSystem fs, FileAttributes attributes) throws IOException{
      if (ranges.isEmpty() || attributes.isDirectory()) {
        return false;
      }
      // walk all the way down in the symlinks until a hard entry is reached
      FileAttributes current = attributes;
      while (current.isSymbolicLink()) {
        current = fs.getFileAttributes(attributes.getSymbolicLink());
      }
      // if hard entry is not a file nor can it be a symlink then it is not readable simply deny matching.
      if (!current.isRegularFile()) {
        return false;
      }

      final Range<Long> fileRange = Range.closedOpen( 0L, attributes.size());

      try (FSInputStream is = fs.open(attributes.getPath())) {
        for(RangeMagics rMagic : ranges) {
          Range<Long> r = rMagic.range;
          if (!fileRange.encloses(r)) {
            continue;
          }
          int len = (int) (r.upperEndpoint() - r.lowerEndpoint());
          byte[] bytes = new byte[len];
          is.setPosition(r.lowerEndpoint());
          IOUtils.readFully(is, bytes);
          for (byte[] magic : rMagic.magics) {
            if (Arrays.equals(magic, bytes)) {
              return true;
            }
          }
        }
      }
      return false;
    }

    private class RangeMagics{
      Range<Long> range;
      byte[][] magics;

      public RangeMagics(MagicString ms) {
        this.range = Range.closedOpen( ms.getOffset(), (long) ms.getBytes().length);
        this.magics = new byte[][]{ms.getBytes()};
      }
    }
  }

}
