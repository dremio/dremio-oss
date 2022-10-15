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

package com.dremio.exec.expr.fn.impl;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;

public class Soundex {
  /**
   * Returns the soundex code for a given string based on Oracle implement
   * <p>
   * The soundex function evaluates expression and returns the most significant letter in
   * the input string followed by a phonetic code. Characters that are not alphabetic are
   * ignored. If expression evaluates to the null value, null is returned.
   * <p>
   * The soundex algorithm works with the following steps:
   * 1. Retain the first letter of the string and drop all other occurrences of a, e, i,
   *  o, u, y, h, w. (let's call them special letters)
   * 2. Replace consonants with digits as follows (after the first letter):
   *  b, f, p, v → 1
   *  c, g, j, k, q, s, x, z → 2
   *  d, t → 3
   *  l → 4
   *  m, n → 5
   *  r → 6
   * 3. If two or more letters with the same number were adjacent in the original name
   *  (before step 1), then omit all but the first. This rule also applies to the first
   *  letter.
   * 4. If the string have too few letters in the word that you can't assign three
   *  numbers, append with zeros until there are three numbers. If you have four or more
   *  numbers, retain only the first three.
   */
  @FunctionTemplate(name = "soundex", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class SoundexFunction implements SimpleFunction {
    @Param
    NullableVarCharHolder in;
    @Output
    NullableVarCharHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errCtx;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      // Array that maps each letter from the alphabet to its corresponding number for the
      // soundex algorithm. ABCDEFGHIJKLMNOPQRSTUVWXYZ -> 01230120022455012623010202
      final byte[] mappings = {'0', '1', '2', '3', '0', '1', '2', '0', '0',
        '2', '2', '4', '5', '5', '0', '1', '2', '6',
        '2', '3', '0', '1', '0', '2', '0', '2'};

      final byte[] outBytea;
      if (in.end <= in.start || in.isSet == 0) {
        out.buffer = buffer = buffer.reallocIfNeeded(0);
        out.start = out.end = 0;
        out.isSet = 1;
      } else {
        String text = com.dremio.exec.expr.fn.impl.StringFunctionUtil.soundexCleanUtf8(in, errCtx);

        int len = text.length();
        if (len == 0) {
          out.buffer = buffer = buffer.reallocIfNeeded(0);
          out.start = out.end = 0;
          out.isSet = 0;
        } else {
          byte[] soundex = new byte[len];
          byte[] ret = new byte[4];

          // Retain the first letter
          ret[0] = (byte) text.charAt(0);
          soundex[0] = '\0';

          int si = 1;
          int ret_len = 1;

          // Replace consonants with digits and special letters with 0
          for (int i = 1; i < len; i++) {
            int c = text.charAt(i) - 65;
            if (mappings[c] != soundex[si - 1]) {
              soundex[si] = mappings[c];
              si++;
            }
          }

          int i = 1;
          // If the saved letter's digit is the same as the resulting first digit, skip it
          if (si > 1) {
            if (mappings[text.charAt(0) - 65] == soundex[1]) {
              i = 2;
            }

            for (; i < si; i++) {
              // If it is a special letter, we ignore, because it has been dropped in first step
              if (soundex[i] != '0') {
                ret[ret_len] = soundex[i];
                ret_len++;
                if (ret_len > 3) {
                  break;
                }
              }
            }
          }

          // If the return have too few numbers, append with zeros until there are three
          if (ret_len <= 3) {
            while (ret_len <= 3) {
              ret[ret_len] = '0';
              ret_len++;
            }
          }

          outBytea = new String(ret).getBytes(com.google.common.base.Charsets.UTF_8);
          out.buffer = buffer = buffer.reallocIfNeeded(outBytea.length);
          out.buffer.setBytes(0, outBytea);
          out.start = 0;
          out.end = outBytea.length;
          out.isSet = 1;
        }
      }
    }
  }
}
