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

import static org.apache.arrow.util.Preconditions.checkArgument;
import static org.apache.arrow.util.Preconditions.checkNotNull;

import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.fn.impl.HashValPair;
import com.dremio.common.expression.fn.impl.MurmurHash3;

import io.netty.util.internal.PlatformDependent;


/**
 * Arrow compliant BloomFilter implementation. The filter expects a 16 byte buf as the key.
 * <b>The class is not thread safe while adding or checking memberships.</b>
 * <p>
 * The bloomfilter implementation uses MurMur3 as hashing algorithm. The same hash value is reused for all keys
 * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and Michael
 * Mitzenmacher. The paper argues that this trick doesn't significantly deteriorate the
 * performance of a Bloom filter (yet only needs two hash functions).
 */
@NotThreadSafe
public class BloomFilter implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(BloomFilter.class);
    private static final double FPP = 0.01;
    private static final int META_BYTES_CNT = 32; // should always be a multiple of 8
    private static final int SEED = 0;

    private BufferAllocator allocator;
    private long sizeInBits;
    private long sizeInBytes;
    private int numHashFunctions;
    private String name;
    private ArrowBuf dataBuffer;
    private long numBitsSetLoc;

    /**
     * Initialise. The dataBuffer memory is used for keeping the bloomfilter bits.
     * The num of hash functions are automatically computed from the datasize assuming a FPP of 0.01.
     *
     * @param bufferAllocator
     * @param name
     * @param minSizeBytes Min size for the filter. Actual size will be the buffer capacity considering the rounding policy used by the allocator.
     */
    public BloomFilter(BufferAllocator bufferAllocator, String name, long minSizeBytes) {
        // Enables filter to do 64 bit operations during merge.
        checkArgument(minSizeBytes % 8==0, "Data size should be multiple of 8 bytes");
        checkArgument(minSizeBytes > META_BYTES_CNT, "Invalid data size");
        checkNotNull(bufferAllocator);

        // Last 32 bytes are used for meta purposes. 24 bytes for name, and 8 bytes for numBitsSet.
        this.sizeInBytes = minSizeBytes - META_BYTES_CNT;
        this.allocator = bufferAllocator;
        this.name = name;
    }

    private BloomFilter(ArrowBuf dataBuffer) {
        setup(dataBuffer);

        byte[] nameBytes = new byte[24];
        this.dataBuffer.getBytes(sizeInBytes, nameBytes);
        this.name = new String(nameBytes, StandardCharsets.UTF_8);
        this.dataBuffer.readerIndex(0);
        this.dataBuffer.writerIndex(dataBuffer.capacity());
        this.numBitsSetLoc = dataBuffer.memoryAddress() + sizeInBytes + META_BYTES_CNT - 8; // last 8 bytes in buffer
    }

    public void setup() {
        checkNotNull(this.allocator, "Setup not required for deserialized objects.");

        this.dataBuffer = this.allocator.buffer(this.sizeInBytes + META_BYTES_CNT);
        setup(dataBuffer);

        // Unset bits explicitly. The sliced buffer might be previously used.
        dataBuffer.writerIndex(0);
        for (int i = 0; i < sizeInBytes; i += 8) {
            dataBuffer.writeLong(0l);
        }

        // Last 32 bytes are meta bytes, including 24 byte name and 8 byte numSetBits.
        byte[] metaBytes = new byte[24];
        byte[] nameBytesAll = name.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(name.getBytes(StandardCharsets.UTF_8), Math.max(0, nameBytesAll.length - 24), metaBytes, 0, Math.min(24, nameBytesAll.length));
        this.name = new String(metaBytes, StandardCharsets.UTF_8);
        this.dataBuffer.writeBytes(metaBytes);
        this.dataBuffer.writeLong(0L);
        this.dataBuffer.readerIndex(0);
        this.numBitsSetLoc = dataBuffer.memoryAddress() + sizeInBytes + META_BYTES_CNT - 8;
        logger.debug("Bloomfilter {} set up completed.", this.name);
    }

    private void setup(ArrowBuf dataBuffer) {
        checkNotNull(dataBuffer);

        final long dataSize = dataBuffer.capacity();
        checkArgument(dataSize % 8==0, "Data size should be multiple of 8 bytes");
        checkArgument(dataSize > META_BYTES_CNT, "Invalid data size");
        this.dataBuffer = dataBuffer;
        this.sizeInBytes = dataSize - META_BYTES_CNT;
        this.sizeInBits = this.sizeInBytes * 8;
        this.numHashFunctions = getOptimalNumOfHashFunctions();
        this.allocator = null;
    }

    public String getName() {
        return name;
    }

    public long getSizeInBytes() {
        return sizeInBytes + META_BYTES_CNT;
    }

    /**
     * Deserialization helper, that prepares the bloomfilter from the input stream.
     * IMPORTANT: BloolFilter::setup should not be called on this filter object.
     *
     * @param dataBuffer ArrowBuf containing bloomfilter set bits
     * @return
     */
    public static BloomFilter prepareFrom(ArrowBuf dataBuffer) {
        return new BloomFilter(dataBuffer);
    }

    /**
     * Returns the data buffer, which has the bloomfilter bits set. The buffer is released when BloomFilter::close is invoked.
     *
     * @return
     */
    public ArrowBuf getDataBuffer() {
        // update numBitsSet
        this.dataBuffer.writerIndex(sizeInBytes + META_BYTES_CNT);
        this.dataBuffer.readerIndex(0);
        return this.dataBuffer;
    }

    /**
     * Checks if a key is currently present in the filter. false result indicates key is not present.
     *
     * @param bloomFilterKey Arrowbuf containg the key, assuming key starts at index zero
     * @param length         length of the key within arrowbuf
     * @return
     */
    public boolean mightContain(ArrowBuf bloomFilterKey, int length) {
        HashValPair hashValPair = MurmurHash3.murmur3_128(0, length, bloomFilterKey, SEED);
        long combinedHash = hashValPair.getHash1();
        for (int i = 0; i < numHashFunctions; i++) {
            // Make the combined hash positive and indexable
            if (!getBit((combinedHash & Long.MAX_VALUE) % sizeInBits)) {
                return false;
            }
            combinedHash += hashValPair.getHash2();
        }
        return true;
    }

    /**
     * Adds the key to the bloomfilter.
     *
     * @param bloomFilterKey Arrowbuf containg the key, assuming key starts at index zero
     * @param length         length of the key within arrowbuf
     * @return Returns true if it caused a change in the state.
     * True return value guarantees that key is inserted for the first time.
     */
    public boolean put(ArrowBuf bloomFilterKey, int length) {
        HashValPair hashValPair = MurmurHash3.murmur3_128(0, length, bloomFilterKey, SEED);
        boolean bitsChanged = false;
        long combinedHash = hashValPair.getHash1();
        long numBitsSet = getNumBitsSet();

        for (int i = 0; i < numHashFunctions; i++) {
            // Make the combined hash positive and indexable
            boolean newBitSet = setBit((combinedHash & Long.MAX_VALUE) % sizeInBits);
            combinedHash += hashValPair.getHash2(); // generate new hash by adding up.

            if (newBitSet) {
                bitsChanged = true;
                numBitsSet++;
            }
        }
        setNumBitsSet(numBitsSet);
        return bitsChanged;
    }

    /**
     * Returns the probability that {@linkplain #mightContain(ArrowBuf, int)}  will erroneously return {@code
     * true} for an object that has not actually been put in the {@code BloomFilter}.
     * <p>
     * FPP increases exponentially when the number of distinct keys in the bloomfilter increase linearly.
     *
     * @return
     */
    public double getExpectedFPP() {
        return Math.pow((double) getNumBitsSet() / sizeInBits, numHashFunctions);
    }

    /**
     * Returns true if expected FPP is greater than five times configured FPP (0.01)
     * @return
     */
    public boolean isCrossingMaxFPP() {
        return getExpectedFPP() > (5 * FPP);
    }

    /**
     * Maximum distinct keys, that can be inserted, to stay below MAX_FPP error probability.
     * If more keys are inserted, the error rate might go beyond permissible limit.
     *
     * @return
     */
    public long getOptimalInsertions() {
        return (long) (-sizeInBits * (Math.log(2) * Math.log(2)) / Math.log(FPP));
    }

    /**
     * Returns optimal size of the filter for "n" insertions and configured FPP.
     *
     * @param expectedInsertions - number of expected insertions
     * @return
     */
    public static long getOptimalSize(long expectedInsertions) {
        checkArgument(expectedInsertions > 0);
        long optimalSize = (long) (-expectedInsertions * Math.log(FPP) / (Math.log(2) * Math.log(2))) / 8;
        optimalSize = ((optimalSize + 8) / 8) * 8; // next multiple of 8
        return optimalSize + META_BYTES_CNT;
    }

    /**
     * Merges the bits from another bloomfilter into this one.
     * This operation will work only if both filters are compatible for merge.
     * <p>
     * The filter passed in the parameter will be accessed in read only mode.
     *
     * @param that
     */
    public void merge(BloomFilter that) {
        synchronized (this.getDataBuffer()) {
            /*
             * The base filter could be shared across different slicing threads on the same node.
             * Each slicing thread could attempt merge in parallel. Hence, putting up synchronized to avoid consistency issues.
             */
            checkArgument(this!=that, "Can't merge with the same BloomFilter object.");
            checkArgument(this.numHashFunctions==that.numHashFunctions, "Incompatible BloomFilter, different hashing technique.");
            checkArgument(this.sizeInBits==that.sizeInBits, "Incompatible BloomFilter, different sizes (%s, %s).", this.sizeInBytes, that.sizeInBytes);

            final long thisMemPos = this.dataBuffer.memoryAddress();
            final long thatMemPos = that.dataBuffer.memoryAddress();
            long numBitsSet = getNumBitsSet();
            for (long bytePos = 0; bytePos < sizeInBytes; bytePos += 8) {
                long thisBits = PlatformDependent.getLong(thisMemPos + bytePos);
                long thatBits = PlatformDependent.getLong(thatMemPos + bytePos);
                long mergedBits = thisBits | thatBits;

                PlatformDependent.putLong(thisMemPos + bytePos, mergedBits);
                numBitsSet += (Long.bitCount(mergedBits) - Long.bitCount(thisBits)); // add newly set bits to the count.
            }
            setNumBitsSet(numBitsSet);
        }
    }

    @VisibleForTesting
    public long getNumBitsSet() {
        return PlatformDependent.getLong(numBitsSetLoc);
    }

    private void setNumBitsSet(final long newVal) {
        PlatformDependent.putLong(numBitsSetLoc, newVal);
    }

    /**
     * Sets the bit at a given position. Returns true if a new bit is set.
     *
     * @param pos
     * @return
     */
    private boolean setBit(long pos) {
        long bytePos = pos / 8;
        long bitOffset = pos % 8;
        byte byteAtPos = dataBuffer.getByte(bytePos);

        // is already set
        if (((byteAtPos >> bitOffset) & 1) == 1) {
            return false;
        }

        byteAtPos |= 1 << bitOffset;
        dataBuffer.writerIndex(bytePos).writeByte(byteAtPos);
        return true;
    }

    /**
     * Returns true if bit is already set at the given position.
     *
     * @param pos
     * @return
     */
    private boolean getBit(long pos) {
        long bytePos = pos / 8;
        long bitOffset = pos % 8;

        byte byteAtPos = dataBuffer.getByte(bytePos);

        return ((byteAtPos >> bitOffset) & 1) == 1;
    }

    /**
     * Computes expected insertions, given max FPP, and optimal no. of hash functions for this set.
     *
     * @return
     */
    private int getOptimalNumOfHashFunctions() {
        long expectedInsertions = getOptimalInsertions();
        return Math.max(1, (int) Math.round((double) sizeInBits / expectedInsertions * Math.log(2)));
    }

    @Override
    public String toString() {
        return "BloomFilter{" +
                "name='" + name + '\'' +
                ", sizeInBytes=" + sizeInBytes +
                ", numHashFunctions=" + numHashFunctions +
                ", numBitsSet=" + getNumBitsSet() +
                ", expectedFpp=" + getExpectedFPP() +
                '}';
    }

    /**
     * Closes the relevant resources.
     */
    @Override
    public void close() {
        logger.debug("Closing bloomfilter {}'s data buffer. RefCount {}", this.name, dataBuffer.refCnt());
        try {
            dataBuffer.close();
        } catch (Exception e) {
            logger.error("Error while closing bloomfilter " + this.name, e);
        }
    }
}
