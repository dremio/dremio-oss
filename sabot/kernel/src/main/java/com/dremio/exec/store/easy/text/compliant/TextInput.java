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
package com.dremio.exec.store.easy.text.compliant;

/*******************************************************************************
 * Copyright 2014 uniVocity Software Pty Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.commons.io.ByteOrderMark;

import com.dremio.common.exceptions.UserException;
import com.dremio.io.CompressedFSInputStream;
import com.dremio.io.FSInputStream;
import com.google.common.base.Preconditions;

import io.netty.util.internal.PlatformDependent;

/**
 * Class that fronts an InputStream to provide a byte consumption interface.
 * Also manages only reading lines to and from each split.
 */
final class TextInput {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextInput.class);

//  private static final int READ_CHARS_LIMIT = 1024*64;
  private final byte[] lineSeparator;
  private final byte normalizedLineSeparator;
  private final TextParsingSettings settings;

  private long lineCount;
  private long charCount;

  /**
   * The starting position in the file.
   */
  private final long startPos;
  private final long endPos;

  private long streamPos;

  private final FSInputStream input;

  private final ArrowBuf buffer;
  private final ByteBuffer underlyingBuffer;
  private final long bStart;
  private final long bStartMinus1;

  /**
   * Whether there was a possible partial line separator on the previous
   * read so we dropped it and it should be appended to next read.
   */
  private int remByte = -1;

  /**
   * The current position in the buffer. (bufferPtr - 1) indicates the number of bytes
   * that have been consumed so far.
   */
  public int bufferPtr;

  /**
   * The quantity of valid data in the buffer. Another way to look at that would be
   * the number of bytes loaded in the most recent chunk read.
   */
  public int length = -1;

  private boolean endFound = false;

  /**
   * Creates a new instance with the mandatory characters for handling newlines transparently.
   * lineSeparator the sequence of characters that represent a newline, as defined in {@link Format#getLineSeparator()}
   * normalizedLineSeparator the normalized newline character (as defined in {@link Format#getNormalizedNewline()}) that is used to replace any lineSeparator sequence found in the input.
   */
  public TextInput(TextParsingSettings settings, FSInputStream input, ArrowBuf readBuffer, long startPos, long endPos) {
    this.lineSeparator = settings.getNewLineDelimiter();
    byte normalizedLineSeparator = settings.getNormalizedNewLine();
    boolean isCompressed = input instanceof CompressedFSInputStream ;
    Preconditions.checkArgument(!isCompressed || startPos == 0, "Cannot use split on compressed stream.");

    // splits aren't allowed with compressed data.  The split length will be the compressed size which means we'll normally end prematurely.
    if(isCompressed && endPos > 0){
      endPos = Long.MAX_VALUE;
    }

    this.input = input;
    this.settings = settings;

    this.startPos = startPos;
    this.endPos = endPos;

    this.normalizedLineSeparator = normalizedLineSeparator;

    this.buffer = readBuffer;
    this.bStart = buffer.memoryAddress();
    this.bStartMinus1 = bStart -1;
    this.underlyingBuffer = buffer.nioBuffer(0, LargeMemoryUtil.checkedCastToInt(buffer.capacity()));
  }

  /**
   * Test the input to position for read start.  If the input is a non-zero split or
   * splitFirstLine is enabled, input will move to appropriate complete line.
   * @throws IOException
   */
  final boolean start() throws IOException {
    lineCount = 0;
    if(startPos > 0) {
      input.setPosition(startPos);
    }

    updateBuffer();
    if (startPos == 0) {
      skipOptionalBOM();
    }
    if (length > 0) {
      if(startPos > 0 ){
        return true;
      }
    }
    return false;
  }


  /**
   * Helper method to get the most recent characters consumed since the last record started.
   * May get an incomplete string since we don't support stream rewind.  Returns empty string for now.
   * @return String of last few bytes.
   * @throws IOException
   */
  public String getStringSinceMarkForError() throws IOException {
    return " ";
  }

  long getPos(){
    return streamPos + bufferPtr;
  }

  /**
   * read some more bytes from the stream.  Uses the zero copy interface if available.  Otherwise, does byte copy.
   * @throws IOException
   */
  private void read() throws IOException {
    if(remByte != -1){
      for (int i = 0; i <= remByte; i++) {
        underlyingBuffer.put(lineSeparator[i]);
      }
      remByte = -1;
    }
    length = input.read(underlyingBuffer);
  }


  /**
   * Read more data into the buffer.  Will also manage split end conditions.
   * @throws IOException
   */
  private void updateBuffer() throws IOException {
    streamPos = input.getPosition();
    underlyingBuffer.clear();

    if(endFound || streamPos > endPos){
      length = -1;
      return;
    }

    read();

    // check our data read allowance.
    if(streamPos + length >= this.endPos){
      updateLengthBasedOnConstraint();
    }

    charCount += bufferPtr;
    bufferPtr = 1;

    buffer.writerIndex(underlyingBuffer.limit());
    buffer.readerIndex(underlyingBuffer.position());

  }

  /**
   * Checks to see if we can go over the end of our bytes constraint on the data.  If so,
   * adjusts so that we can only read to the last character of the first line that crosses
   * the split boundary.
   */
  private void updateLengthBasedOnConstraint() {
    final long max = bStart + length;
    for(long m = bStart + (endPos - streamPos); m < max; m++) {
      for (int i = 0; i < lineSeparator.length; i++) {
        long mPlus = m + i;
        if (mPlus < max) {
          // we found a line separator and don't need to consult the next byte.
          if (lineSeparator[i] == PlatformDependent.getByte(mPlus) && i == lineSeparator.length - 1) {
            length = (int) (mPlus - bStart) + 1;
            endFound = true;
            return;
          }
        } else {
          // the last N characters of the read were remnant bytes. We'll hold off on dealing with these bytes until the next read.
          remByte = i;
          length = length - i;
          return;
        }
      }
    }
  }

  boolean match(byte byteChar, byte[] parameter) throws IOException {
    if (byteChar == parameter[0]) {
      int bufferPtrTemp = bufferPtr - 1;
      for (int i = 1; i < parameter.length; i++, bufferPtrTemp++) {
        if (parameter[i] != buffer.getByte(bufferPtrTemp)) {
          return false;
        }
      }

      // we don't need to update buffer position if parameter is one byte long
      if (parameter.length > 1) {
        bufferPtr += (parameter.length - 1);
        if (bufferPtr > length) {
          if (length != -1) {
            updateBuffer();
          } else {
            throw StreamFinishedPseudoException.INSTANCE;
          }
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Get next byte from stream.  Also maintains the current line count.  Will throw a StreamFinishedPseudoException
   * when the stream has run out of bytes.
   * @return next byte from stream.
   * @throws IOException Reached End of Input
   */
  public byte[] nextChar() throws IOException {
    byte[] byteNType =  new byte[2];
    byte byteChar = nextCharNoNewLineCheck();
    byteNType[1] = byteChar;
    byte[] fieldSeparator = settings.getDelimiter();
    if (match(byteChar, lineSeparator)) {
      byteNType[0] = 1;
      lineCount++;
      byteNType[1] = -1;
    } else if (byteChar == normalizedLineSeparator) {
      lineCount++;
      byteNType[0] = 1;
    } else if (match(byteChar, fieldSeparator)) {
      byteNType[0] = 2;
    }

    return byteNType;
  }

  /**
   * Get next byte from stream.  Do no maintain any line count  Will throw a StreamFinishedPseudoException
   * when the stream has run out of bytes.
   * @return next byte from stream.
   * @throws IOException
   */
  public final byte nextCharNoNewLineCheck() throws IOException {

    if (length == -1) {
      throw StreamFinishedPseudoException.INSTANCE;
    }

    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      buffer.checkBytes(bufferPtr - 1, bufferPtr);
    }

    byte byteChar = PlatformDependent.getByte(bStartMinus1 + bufferPtr);

    if (bufferPtr >= length) {
      if (length != -1) {
        updateBuffer();
        bufferPtr--;
      } else {
        throw StreamFinishedPseudoException.INSTANCE;
      }
    }

    bufferPtr++;

    return byteChar;
  }

  /**
   * Number of lines read since the start of this split.
   * @return
   */
  public final long lineCount() {
    return lineCount;
  }

  /**
   * Skip forward the number of line delimiters.  If you are in the middle of a line,
   * a value of 1 will skip to the start of the next record.
   * @param lines Number of lines to skip.
   * @throws IOException
   */
  public final void skipLines(int lines) throws IOException {
    if (lines < 1) {
      return;
    }
    long expectedLineCount = this.lineCount + lines;

    try {
      do {
        nextChar();
      } while (lineCount < expectedLineCount /*&& bufferPtr < READ_CHARS_LIMIT*/);
      if (lineCount < lines) {
        throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
      }
    } catch (EOFException ex) {
      throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
    }
  }

  // Check if the input stream has a specific byte-order-mark (BOM)
  private final boolean checkBom(ByteOrderMark bom) {
    int bomLength = bom.length();
    if (bufferPtr + bomLength >= length) {
      // Not enough bytes from the current position to the end of the buffer
      return false;
    }
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      buffer.checkBytes(bufferPtr - 1, bufferPtr + bomLength);
    }

    byte[] bomBytes = bom.getBytes();
    for (int i = 0; i < bomLength; i++) {
      byte nextChar = PlatformDependent.getByte(bStartMinus1 + bufferPtr + i);
      if (nextChar != bomBytes[i]) {
        // No BOM. Position is unchanged
        return false;
      }
    }
    return true;
  }

  // Check if the input stream has a byte-order-mark (BOM). Skip it if it's there
  private final void skipOptionalBOM() throws IOException {
    if (checkBom(ByteOrderMark.UTF_8)) {
      bufferPtr += ByteOrderMark.UTF_8.length();
    } else if (checkBom(ByteOrderMark.UTF_16LE) || checkBom(ByteOrderMark.UTF_16BE)) {
      throw UserException.dataReadError()
        .message("UTF-16 files not supported")
        .build(logger);
    }
  }

  public final long charCount() {
    return charCount + bufferPtr;
  }

  public long getLineCount() {
    return lineCount;
  }

  public void close() throws IOException{
    input.close();
    // Prevent downstream consumers to read pre-loaded chunk of bytes after closing.
    length = -1;
  }
}
