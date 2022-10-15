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

package com.dremio.exec.util;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Map.Entry.comparingByValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Attempts for a fair distribution of available buffer length for portions of the given keys.
 * Expected to be used by RuntimeFiltering when composite keys are in picture. The class will be used to ensure there
 * is a fair representation of all keys within the feed going for the hash.
 */
public class KeyFairSliceCalculator {
    private static final Comparator<Map.Entry<String, Integer>> VALUE_COMPARATOR = comparingByValue();

    // Map of key names and respective target slice size of the key.
    private Map<String, Integer> keySlices = new HashMap();
    private int totalSize;
    private int totalValiditySize;

    // Set true if any of the keys are not fully used.
    private boolean keysToBeSliced = false;

    /**
     * The function first sorts all the entries by key sizes. If total of all key sizes exceeds the cap (maxTotalSize),
     * we calculate fair allocation by dividing max size with number of keys.
     *
     * In case key size is less than the fair allocation, remaining length is equally distributed to keys of larger sizes.
     *
     * @param originalKeySizes Original keysizes, Integer.MAX_VALUE for variable length fields and -1 for bit type keys
     * @param maxTotalSize     Max size in which all key slices should fit. Total size can be less than max size in case total size of all keys is less than maxTotalSize.
     */
    public KeyFairSliceCalculator(Map<String, Integer> originalKeySizes, int maxTotalSize) {
        int numKeys = originalKeySizes.size();
        List<Map.Entry<String, Integer>> listKeySizes = new ArrayList<>(originalKeySizes.entrySet());
        int numBooleanKeys = (int) listKeySizes.stream().filter(p -> p.getValue() == -1).count();
        int numNonBooleanKeys = numKeys - numBooleanKeys;
        int numValidityBytes = (numKeys + numBooleanKeys + 7) / 8;
        int numValueBytes = maxTotalSize - numValidityBytes;

        checkArgument(numValueBytes >= numNonBooleanKeys,
          "Not possible to fit %s keys (%s boolean) in %s bytes", numKeys, numBooleanKeys, maxTotalSize);

        Collections.sort(listKeySizes, VALUE_COMPARATOR); // Assuming Collections.sort will maintain original order when values are equal

        int perKeyAllocation = numNonBooleanKeys == 0 ? 0 : numValueBytes / numNonBooleanKeys;
        int remainder = numNonBooleanKeys == 0 ? numValueBytes : numValueBytes % numNonBooleanKeys;

        int unprocessedKeys = numNonBooleanKeys;
        for (Map.Entry<String, Integer> keySize : listKeySizes) {
            if (keySize.getValue() == -1) { // bit value
                keySlices.put(keySize.getKey(), 0);
            } else if (perKeyAllocation >= keySize.getValue()) {
                remainder += perKeyAllocation - keySize.getValue();
                keySlices.put(keySize.getKey(), keySize.getValue());
                unprocessedKeys--;
            } else {
                int remainderPortion = remainder / unprocessedKeys;
                remainderPortion = Math.min(remainderPortion, (keySize.getValue() - perKeyAllocation));
                keySlices.put(keySize.getKey(), perKeyAllocation + remainderPortion);
                remainder -= remainderPortion;
                keysToBeSliced = true;
                unprocessedKeys--;
            }
        }
        this.totalValiditySize = numValidityBytes;
        this.totalSize = numValueBytes - remainder + numValidityBytes;
    }

    /**
     * Slice calculated for a given key
     *
     * @param key
     * @return
     */
    public Integer getKeySlice(String key) {
        return keySlices.get(key);
    }

    /**
     * Return total size of all slices. This will never exceed the maxTotalSize.
     *
     * @return
     */
    public int getTotalSize() {
        return totalSize;
    }

    /**
     * Returns true if key lengths have to be sliced in order to fit within max size
     * @return
     */
    public boolean keysTrimmed() {
        return keysToBeSliced;
    }

    /**
     * Return the number of bytes required to hold the validity bits of the keys
     * @return
     */
    public int numValidityBytes() {
        return totalValiditySize;
      }
}
