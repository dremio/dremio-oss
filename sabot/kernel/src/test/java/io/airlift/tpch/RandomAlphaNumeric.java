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

public class RandomAlphaNumeric
        extends AbstractRandomInt
{
    private static final char[] ALPHA_NUMERIC = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,".toCharArray();

    private static final double LOW_LENGTH_MULTIPLIER = 0.4;
    private static final double HIGH_LENGTH_MULTIPLIER = 1.6;

    private static final int USAGE_PER_ROW = 9;

    private final int minLength;
    private final int maxLength;

    public RandomAlphaNumeric(long seed, int averageLength)
    {
        this(seed, averageLength, 1);
    }

    public RandomAlphaNumeric(long seed, int averageLength, int expectedRowCount)
    {
        super(seed, USAGE_PER_ROW * expectedRowCount);
        this.minLength = (int) (averageLength * LOW_LENGTH_MULTIPLIER);
        this.maxLength = (int) (averageLength * HIGH_LENGTH_MULTIPLIER);
    }

    public String nextValue()
    {
        char[] buffer = new char[nextInt(minLength, maxLength)];

        long charIndex = 0;
        for (int i = 0; i < buffer.length; i++) {
            if (i % 5 == 0) {
                charIndex = nextInt(0, Integer.MAX_VALUE);
            }
            buffer[i] = ALPHA_NUMERIC[(int) (charIndex & 0x3f)];
            charIndex >>= 6;
        }

        return new String(buffer);
    }
}
