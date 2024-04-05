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

package com.dremio.sabot.op.common.ht2;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.util.KeyFairSliceCalculator;
import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldMode;
import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Prepares the key for join runtime filter by accepting and parsing keys in the HashTable, only for
 * the required fields. The filter can be used only in the HashJoin scenarios where the keys are
 * present in {@link com.dremio.sabot.op.common.ht2.HashTable}. The functionality uses a fixed
 * ArrowBuf and uses the same for delivering all parsed keys. The keys are loaded in same space.
 */
public class HashTableKeyReader implements AutoCloseable {
  private ArrowBuf keyBuf;
  private int keyBufSize = 32; // max
  private List<KeyPosition> keyPositions;
  private int keyValidityBytes;
  private boolean isKeyTrimmedToFitSize = false;
  private boolean setVarFieldLenInFirstByte = false;

  private HashTableKeyReader() {
    // Always use builder
  }

  public int getKeyBufSize() {
    return keyBufSize;
  }

  /**
   * Size after removing the validity bytes
   *
   * @return
   */
  public byte getEffectiveKeySize() {
    return (byte) (getKeyBufSize() - keyValidityBytes);
  }

  public boolean isKeyTrimmedToFitSize() {
    return isKeyTrimmedToFitSize;
  }

  /**
   * Load the next composite key in the key buffer. The keys are trimmed in case total size exceeds
   * the limit according to the logic described in {@link KeyFairSliceCalculator}.
   *
   * <p>Caller is expected to provide the fixedAddr and variableAddr for this new key
   */
  public void loadNextKey(long currentFixedAddr, long currentVarAddr) {
    // zero out the key buf
    keyBuf.setBytes(0, new byte[this.keyBufSize]);

    int keyBufValueOffset = keyValidityBytes;
    int keyBufValidityBitOffset = 0;
    int keyBufValidityByteOffset = 0;

    for (KeyPosition keyPosition : keyPositions) {
      boolean valid =
          ((PlatformDependent.getInt(currentFixedAddr + keyPosition.nullByteOffset)
                      >>> keyPosition.nullBitOffset)
                  & 1)
              == 1;
      if (valid) {
        // write validity bit
        PlatformDependent.putByte(
            keyBuf.memoryAddress() + keyBufValidityByteOffset,
            (byte)
                (PlatformDependent.getByte(keyBuf.memoryAddress() + keyBufValidityByteOffset)
                    | (byte) (1 << keyBufValidityBitOffset)));
        // write value
        switch (keyPosition.fieldType) {
          case BIT:
            Preconditions.checkArgument(keyPosition.offset == keyPosition.nullBitOffset + 1);
            int value =
                (PlatformDependent.getInt(currentFixedAddr + keyPosition.nullByteOffset)
                        >>> keyPosition.offset)
                    & 1;

            keyBufValidityByteOffset += (keyBufValidityBitOffset + 1) / Byte.SIZE;
            keyBufValidityBitOffset = (keyBufValidityBitOffset + 1) % Byte.SIZE;
            PlatformDependent.putByte(
                keyBuf.memoryAddress() + keyBufValidityByteOffset,
                (byte)
                    (PlatformDependent.getByte(keyBuf.memoryAddress() + keyBufValidityByteOffset)
                        | (byte) (value << keyBufValidityBitOffset)));
            break;
          case FIXED:
            Copier.copy(
                currentFixedAddr + keyPosition.offset,
                keyBuf.memoryAddress() + keyBufValueOffset,
                keyPosition.allocatedLength);
            break;
          case VARIABLE:
            // Variable data is arranged in following fashion -
            // [row_len|key1_len|key1|key2_len|key2|...] // Each length field is a 4 byte int
            // Navigate to the required key position within the block
            long keyAddr = currentVarAddr + 4;
            int keySequenceNo = keyPosition.offset;
            while (--keySequenceNo > 0) {
              keyAddr += (4 + PlatformDependent.getInt(keyAddr));
            }
            int keyFieldLength = PlatformDependent.getInt(keyAddr);
            int allocatedLen = keyPosition.allocatedLength;
            // prefix pad with 0 bytes if data value is shorter than allocation
            int keySizeForCopy = Math.min(allocatedLen, keyFieldLength);
            int offset = keyBufValueOffset;
            if (setVarFieldLenInFirstByte) {
              keySizeForCopy =
                  (keySizeForCopy == allocatedLen) ? keySizeForCopy - 1 : keySizeForCopy;
              keyBuf.setByte(offset++, (byte) keySizeForCopy);
              allocatedLen--;
            }
            Copier.copy(
                keyAddr + 4,
                keyBuf.memoryAddress() + offset + allocatedLen - keySizeForCopy,
                keySizeForCopy);
            break;
        }
      }
      keyBufValueOffset += keyPosition.allocatedLength;
      // If invalid boolean value advance by 2 bits else by 1 bit
      keyBufValidityByteOffset +=
          (keyBufValidityBitOffset + ((!valid && keyPosition.fieldType == FieldMode.BIT) ? 2 : 1))
              / Byte.SIZE;
      keyBufValidityBitOffset =
          (keyBufValidityBitOffset + ((!valid && keyPosition.fieldType == FieldMode.BIT) ? 2 : 1))
              % Byte.SIZE;
    }
  }

  public ArrowBuf getKeyHolder() {
    return keyBuf;
  }

  /**
   * Key without validity byte
   *
   * @return
   */
  public ArrowBuf getKeyValBuf() {
    return keyBuf.slice(keyValidityBytes, keyBufSize - keyValidityBytes);
  }

  /**
   * Checks the validity byte if all values are null.
   *
   * @return
   */
  public boolean areAllValuesNull() {
    byte[] validityBytes = new byte[keyValidityBytes];
    keyBuf.getBytes(0, validityBytes);
    for (byte validityByte : validityBytes) {
      if (validityByte != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(keyBuf);
  }

  public static class Builder {
    private final HashTableKeyReader keyReader = new HashTableKeyReader();
    private List<String> fieldNames = new ArrayList<>();
    private PivotDef pivot;
    private BufferAllocator bufferAllocator;
    private int maxKeySize = 32;

    public Builder setFieldsToRead(List<String> fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public Builder setMaxKeySize(int maxKeySize) {
      this.maxKeySize = maxKeySize;
      return this;
    }

    public Builder setPivot(PivotDef pivot) {
      this.pivot = pivot;
      return this;
    }

    public Builder setBufferAllocator(BufferAllocator bufferAllocator) {
      this.bufferAllocator = bufferAllocator;
      return this;
    }

    public Builder setSetVarFieldLenInFirstByte(boolean setVarFieldLenInFirstByte) {
      keyReader.setVarFieldLenInFirstByte = setVarFieldLenInFirstByte;
      return this;
    }

    public HashTableKeyReader build() {
      checkArgument(CollectionUtils.isNotEmpty(fieldNames), "Fields to read cannot be empty");
      checkNotNull(pivot);

      keyReader.keyPositions = new ArrayList<>(fieldNames.size());

      prepareKeyPositions(fieldNames);
      sliceKeyLenToFitInMaxSize();
      try {
        // If you want to initialise another closeable here, ensure closure at error cases when
        // resources are partially initialised.
        keyReader.keyBuf = bufferAllocator.buffer(keyReader.keyBufSize);
        return keyReader;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    private void sliceKeyLenToFitInMaxSize() {
      // Take slices of keys so total key fits in 16 bytes
      final Map<String, Integer> keySizes =
          keyReader.keyPositions.stream()
              .collect(
                  Collectors.toMap(
                      KeyPosition::getKey,
                      KeyPosition::getAllocatedLength,
                      (x, y) -> y,
                      LinkedHashMap::new));
      final KeyFairSliceCalculator fairSliceCalculator =
          new KeyFairSliceCalculator(keySizes, maxKeySize);
      keyReader.keyPositions.stream()
          .forEach(k -> k.setAllocatedLength(fairSliceCalculator.getKeySlice(k.getKey())));
      keyReader.keyBufSize = fairSliceCalculator.getTotalSize();
      keyReader.isKeyTrimmedToFitSize = fairSliceCalculator.keysTrimmed();
      keyReader.keyValidityBytes = fairSliceCalculator.numValidityBytes();
    }

    private void prepareKeyPositions(List<String> fieldNames) {
      for (String fieldName : fieldNames) {
        // Search in fixed width fields
        Optional<KeyPosition> fixedWidthColKeyPosition = findKeyPositionInFixedWidthCols(fieldName);
        if (fixedWidthColKeyPosition.isPresent()) {
          keyReader.keyPositions.add(fixedWidthColKeyPosition.get());
          continue;
        }

        // Search in variable width columns.
        keyReader.keyPositions.add(
            findKeyPositionInVarWidthCols(fieldName)
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Field name " + fieldName + " not find in pivot.")));
      }
    }

    private Optional<KeyPosition> findKeyPositionInVarWidthCols(String fieldName) {
      // Search in variable width fields
      int sequencePosition = 1;
      for (VectorPivotDef vectorPivotDef : this.pivot.getVariablePivots()) {
        final Field field = vectorPivotDef.getIncomingVector().getField();
        if (field.getName().equalsIgnoreCase(fieldName)) {
          checkArgument(
              !field.getType().isComplex(), "Runtime filter not supported on complex type fields");
          return Optional.of(
              new KeyPosition(
                  fieldName,
                  sequencePosition,
                  vectorPivotDef.getNullByteOffset(),
                  vectorPivotDef.getNullBitOffset(),
                  Integer.MAX_VALUE,
                  FieldMode.VARIABLE));
        }
        sequencePosition++;
      }
      return Optional.empty();
    }

    private Optional<KeyPosition> findKeyPositionInFixedWidthCols(String fieldName) {
      for (VectorPivotDef vectorPivotDef : this.pivot.getFixedPivots()) {
        final Field field = vectorPivotDef.getIncomingVector().getField();
        if (field.getName().equalsIgnoreCase(fieldName)) {
          checkArgument(
              !field.getType().isComplex(), "Runtime filter not supported on complex type fields");
          return Optional.of(
              new KeyPosition(
                  fieldName,
                  vectorPivotDef.getOffset(),
                  vectorPivotDef.getNullByteOffset(),
                  vectorPivotDef.getNullBitOffset(),
                  vectorPivotDef.getByteSize(),
                  vectorPivotDef.getMode()));
        }
      }

      return Optional.empty();
    }
  }

  private static class KeyPosition {
    private final String key;
    private final int offset;
    private final int nullByteOffset;
    private final int nullBitOffset;
    private int allocatedLength;
    private final FieldMode fieldType;

    private KeyPosition(
        String key,
        int offset,
        int nullByteOffset,
        int nullBitOffset,
        int allocatedLength,
        FieldMode fieldType) {
      this.key = key;
      this.offset = offset;
      this.nullByteOffset = nullByteOffset;
      this.nullBitOffset = nullBitOffset;
      this.allocatedLength = allocatedLength;
      this.fieldType = fieldType;
    }

    public void setAllocatedLength(int allocatedLength) {
      this.allocatedLength = allocatedLength;
    }

    public String getKey() {
      return key;
    }

    public int getAllocatedLength() {
      return allocatedLength;
    }

    @Override
    public String toString() {
      return "KeyPosition{" + "offset=" + offset + ", length=" + allocatedLength + '}';
    }
  }
}
