/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
/*
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
package io.airlift.tpch;

import static com.google.common.base.Preconditions.checkState;

public class RandomLong
{
    private static final long MULTIPLIER = 6364136223846793005L;
    private static final long INCREMENT = 1;

    private final int expectedUsagePerRow;

    private long seed;
    private int usage;

    /**
     * Creates a new random number generator with the specified seed and
     * specified number of random values per row.
     */
    public RandomLong(long seed, int expectedUsagePerRow)
    {
        this.seed = seed;
        this.expectedUsagePerRow = expectedUsagePerRow;
    }

    /**
     * Get a random value between lowValue (inclusive) and highValue (inclusive).
     */
    protected long nextLong(long lowValue, long highValue)
    {
        nextRand();

        long valueInRange = Math.abs(seed) % (highValue - lowValue + 1);

        return lowValue + valueInRange;
    }

    protected long nextRand()
    {
        if (!(usage < expectedUsagePerRow)) {
            checkState(false, "Expected random to be used only %s times per row", expectedUsagePerRow);
        }
        seed = (seed * MULTIPLIER) + INCREMENT;
        usage++;
        return seed;
    }

    /**
     * Advances the random number generator to the start of the sequence for
     * the next row.  Each row uses a specified number of random values, so the
     * random number generator can be quickly advanced for partitioned data
     * sets.
     */
    public void rowFinished()
    {
        advanceSeed32(expectedUsagePerRow - usage);
        usage = 0;
    }

    /**
     * Advance the specified number of rows.  Advancing to a specific row is
     * needed for partitioned data sets.
     */
    public void advanceRows(long rowCount)
    {
        // finish the current row
        if (usage != 0) {
            rowFinished();
        }

        // advance the seed
        advanceSeed32(expectedUsagePerRow * rowCount);
    }

    //
    // TPC-H uses the 32bit code for advancing 64bit randoms for some reason, so
    // the following code is not used
    //
    public void advanceSeed(long count)
    {
        if (count == 0) {
            return;
        }

        // Recursively compute X(n) = A * X(n-1) + C
        //
        // explicitly:
        // X(n) = A^n * X(0) + { A^(n-1) + A^(n-2) + ... A + 1 } * C
        //
        // we write this as:
        // X(n) = aPow(n) * X(0) + dSum(n) * C
        //
        // we use the following relations:
        // aPow(n) = A^(n%2)*aPow(n/2)*aPow(n/2)
        // dSum(n) =   (n%2)*aPow(n/2)*aPow(n/2) + (aPow(n/2) + 1) * dSum(n/2)
        //

        // first get the highest non-zero bit
        int numberOfBits = 0;
        while (count >> numberOfBits != INCREMENT) {
            numberOfBits++;
        }

        long aPow = MULTIPLIER;
        long dSum = INCREMENT;
        while (--numberOfBits >= 0) {
            dSum *= (aPow + 1);
            aPow = aPow * aPow;

            // odd value
            if (((count >> numberOfBits) % 2) == 1) {
                dSum += aPow;
                aPow *= MULTIPLIER;
            }
        }
        seed = seed * aPow + dSum * INCREMENT;
    }


    private static final long MULTIPLIER_32 = 16807;
    private static final long MODULUS_32 = 2147483647;

    private void advanceSeed32(long count)
    {
        long multiplier = MULTIPLIER_32;
        while (count > 0) {
            // testing for oddness, this seems portable
            if (count % 2 != 0) {
                seed = (multiplier * seed) % MODULUS_32;
            }
            // integer division, truncates
            count = count / 2;
            multiplier = (multiplier * multiplier) % MODULUS_32;
        }
    }
}
