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

import static com.dremio.common.util.DremioStringUtils.isHexDigit;
import static com.dremio.common.util.DremioStringUtils.toBinaryFromHex;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.dremio.exec.expr.fn.FunctionErrorContext;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

public class StringFunctionUtil {

  /* Decode the input bytebuf using UTF-8, and return the number of characters
   */
  public static int getUTF8CharLength(ByteBuf buffer, int start, int end, final FunctionErrorContext
    errCtx) {
    int charCount = 0;

    for (int idx = start, charLen = 0; idx < end; idx += charLen) {
      charLen = utf8CharLen(buffer, idx, errCtx);
      ++charCount;  //Advance the counter, since we find one char.
    }
    return charCount;
  }

  /* Decode the input bytebuf using UTF-8. Search in the range of [start, end], find
   * the position of the first byte of next char after we see "charLength" chars.
   *
   */
  public static int getUTF8CharPosition(ByteBuf buffer, int start, int end, int charLength, final
  FunctionErrorContext errCtx) {
    int charCount = 0;

    if (start >= end) {
      return -1;  //wrong input here.
    }

    for (int idx = start, charLen = 0; idx < end; idx += charLen) {
      charLen = utf8CharLen(buffer, idx, errCtx);
      ++charCount;  //Advance the counter, since we find one char.
      if (charCount == charLength + 1) {
        return idx;
      }
    }
    return end;
  }

  public static Pattern compilePattern(String regex, FunctionErrorContext errCtx) {
    try {
      return Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      throw errCtx.error()
        .message("Invalid regex expression '%s'", regex)
        .addContext("exception", e.getMessage())
        .build();
    }
  }

  public static Pattern compilePattern(String regex, int flags, FunctionErrorContext errCtx) {
    try {
      return Pattern.compile(regex, flags);
    } catch (PatternSyntaxException e) {
      throw errCtx.error()
        .message("Invalid regex expression '%s'", regex)
        .addContext("exception", e.getMessage())
        .build();
    }
  }

  public static int stringLeftMatchUTF8(ByteBuf str, int strStart, int strEnd,
                                        ByteBuf substr, int subStart, int subEnd) {
    return stringLeftMatchUTF8(str, strStart, strEnd, substr, subStart, subEnd, 0);
  }

  public static int stringLeftMatchUTF8(ByteBuf str, int strStart, int strEnd,
                                        ByteBuf substr, int subStart, int subEnd, int offset) {
    for (int i = strStart + offset; i <= strEnd - (subEnd - subStart); i++) {
      int j = subStart;
      for (; j< subEnd; j++) {
        if (str.getByte(i + j - subStart) != substr.getByte(j)) {
          break;
        }
      }

      if (j == subEnd  && j!= subStart) {  // found a matched substr (non-empty) in str.
        return i;   // found a match.
      }
    }

    return -1;
  }

  public static int parseBinaryStringNoFormat(ByteBuf str, int strStart, int strEnd, ByteBuf out,
                                              FunctionErrorContext errCtx) {
    int dstEnd = 0;

    if(((strStart - strEnd) % 2) != 0){
      throw errCtx.error()
        .message("Failure parsing hex string, length was not a multiple of two.")
        .build();
    }
    for (int i = strStart; i < strEnd; i+=2) {
      byte b1 = str.getByte(i);
      byte b2 = str.getByte(i+1);
      if(isHexDigit(b1) && isHexDigit(b2)){
        byte finalByte = (byte) ((toBinaryFromHex(b1) << 4) + toBinaryFromHex(b2));
        out.setByte(dstEnd++, finalByte);
      }else{
        throw errCtx.error()
          .message("Failure parsing hex string, one or more bytes was not a valid hex value.")
          .build();
      }
    }
    return dstEnd;
  }

  /**
   * Returns the length of the valid UTF-8 character sequence that starts at offset 'idx' of 'buffer'
   */
  private static int utf8CharLenNoThrow(ByteBuf buffer, int idx) {
    byte firstByte = buffer.getByte(idx);
    if (firstByte >= 0) { // 1-byte char. First byte is 0xxxxxxx.
      return 1;
    } else if ((firstByte & 0xE0) == 0xC0) { // 2-byte char. First byte is 110xxxxx
      return 2;
    } else if ((firstByte & 0xF0) == 0xE0) { // 3-byte char. First byte is 1110xxxx
      return 3;
    } else if ((firstByte & 0xF8) == 0xF0) { //4-byte char. First byte is 11110xxx
      return 4;
    }
    return 0;
  }

    /**
     * Find the length of the UTF-8 character sequence at a given offset in a buffer
     * @return the UTF-8 character sequence length at offset 'idx' in 'buffer'
     * @throws com.dremio.common.exceptions.UserException if the first byte is not a valid encoded UTF-8 sequence
     */
  public static int utf8CharLen(ByteBuf buffer, int idx, final FunctionErrorContext errCtx) {
    int seqLen = utf8CharLenNoThrow(buffer, idx);
    if (seqLen != 0) {
      return seqLen;
    }
    throw errCtx.error()
      .message("Unexpected byte 0x%s at position %d encountered while decoding UTF8 string.", Integer.toString(buffer.getByte(idx) & 0xff, 16), idx)
      .build();
  }

  /**
   * Copy only the UTF8 symbols from a binary buffer to a destination buffer
   * @param in source of the copy
   * @param start index in 'in' where the copy will begin
   * @param end one past the index in 'in' where the copy will end
   * @param out the destination buffer, that will receive the UTF8 symbols. The buffer is assumed to be properly sized
   *            The buffer's reader- and writer-index will be set appropriately.
   * @return the number of bytes in 'out' that were populated
   */
  public static int copyUtf8(ByteBuf in, final int start, final int end, ArrowBuf out) {
    int i = 0;
    int errBytes = 0;
    while (start + i < end) {
      // Optimize for long runs of ASCII sequences
      byte b = in.getByte(start + i);
      if (b >= 0) {
        out.setByte(i - errBytes, b);
        i++;
        continue;
      }
      int seqLen = utf8CharLenNoThrow(in, start + i);
      if (seqLen == 0 || (start + i + seqLen) > end || !GuavaUtf8.isUtf8(in, start + i, start +
        i + seqLen)) {
        errBytes++;
        i++; // skip the error character
      } else {
        for (int j = i; j < i + seqLen; j++) {
          out.setByte(j - errBytes, in.getByte(start + j));
        }
        i += seqLen;
      }
    }
    return end - start - errBytes;
  }

  /**
   * Copy the UTF8 symbols from 'in' to 'out'. Any non-UTF8 symbols in 'in' will be replaced by 'replacement'
   * @param in  source of the copy
   * @param start index in 'in' where the copy will begin
   * @param end one past the index in 'in' where the copy will end
   * @param out the destination buffer, that will receive copy/replace. The buffer is assumed to be properly sized
   *            The buffer's reader- and writer-index will be set appropriately.
   * @param replacement replacement value for non-UTF8 symbols in 'in'
   * @return the number of bytes in 'out' that were populated
   */
  public static int copyReplaceUtf8(ByteBuf in, final int start, final int end, ByteBuf out, byte
    replacement) {
    int i = 0;
    while (start + i < end) {
      // Optimize for long runs of ASCII sequences
      byte b = in.getByte(start + i);
      if (b >= 0) {
        out.setByte(i, b);
        i++;
        continue;
      }
      int seqLen = utf8CharLenNoThrow(in, start + i);
      if (seqLen == 0 || (start + i + seqLen) > end || !GuavaUtf8.isUtf8(in, start + i, start +
        i + seqLen)) {
        // Replace the non-UTF-8 character with the replacement character
        out.setByte(i, replacement);
        i++;
      } else {
        for (int j = i; j < i + seqLen; j++) {
          out.setByte(j, in.getByte(start + j));
        }
        i += seqLen;
      }
    }
    return end - start;
  }
}
