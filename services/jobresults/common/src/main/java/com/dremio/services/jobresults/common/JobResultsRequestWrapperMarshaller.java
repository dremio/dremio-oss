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
package com.dremio.services.jobresults.common;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.grpc.DrainableByteBufInputStream;
import com.dremio.service.grpc.StreamToByteBufReader;
import com.dremio.service.jobresults.JobResultsRequest;
import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import io.grpc.MethodDescriptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.NettyArrowBuf;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.patch.ArrowByteBufAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;

/**
 * Marshaller of {@link JobResultsRequestWrapper} to avoid job results being copied to heap memory
 * before copying it to grpc on-wire.
 */
public class JobResultsRequestWrapperMarshaller
    implements MethodDescriptor.Marshaller<JobResultsRequestWrapper>, Closeable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JobResultsRequestWrapperMarshaller.class);

  private static final int HEADER_TAG =
      (JobResultsRequest.HEADER_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private static final int SEQUENCE_ID_TAG =
      (JobResultsRequest.SEQUENCEID_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_VARINT;

  private static final int DATA_TAG =
      (JobResultsRequest.DATA_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private static final int FOREMAN_TAG =
      (JobResultsRequest.FOREMAN_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private final BufferAllocator allocator;
  // maintaining inputStreams, so that it can be closed explicitly if inputStream is not closed
  // in-case of server-side errors/exception not reading from inputStream.
  private final List<DrainableByteBufInputStream> inputStreams = new ArrayList<>();

  public JobResultsRequestWrapperMarshaller(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public InputStream stream(JobResultsRequestWrapper value) {
    logger.debug("requestWrapper stream called.");
    cleanupBefore();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ) {
      CodedOutputStream cos = CodedOutputStream.newInstance(baos);

      cos.writeMessage(JobResultsRequest.HEADER_FIELD_NUMBER, value.getHeader());

      cos.writeInt64(JobResultsRequest.SEQUENCEID_FIELD_NUMBER, value.getSequenceId());

      cos.writeMessage(JobResultsRequest.FOREMAN_FIELD_NUMBER, value.getForeman());

      if (value.getByteBuffersLength() > 0) {
        cos.writeTag(JobResultsRequest.DATA_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        cos.writeUInt32NoTag(LargeMemoryUtil.checkedCastToInt(value.getByteBuffersLength()));
      }

      cos.flush();
      ArrowBuf firstBuf = allocator.buffer(baos.size());
      firstBuf.writeBytes(baos.toByteArray());
      List<ByteBuf> buffers = Lists.newArrayList(NettyArrowBuf.unwrapBuffer(firstBuf));

      if (value.getByteBuffersLength() > 0) {
        buffers.addAll(Arrays.asList(value.getByteBuffers()));
      }

      CompositeByteBuf compositeByteBuf =
          new CompositeByteBuf(new ArrowByteBufAllocator(allocator), true, buffers.size(), buffers);

      // buffer ownership transferred to the stream. Decrements refcount when done.
      DrainableByteBufInputStream inputStream = new DrainableByteBufInputStream(compositeByteBuf);
      inputStreams.add(inputStream);
      return inputStream;
    } catch (Exception e) {
      String errorMsg = "Error serializing the jobResultsRequest on-wire grpc stream.";
      logger.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  @Override
  public JobResultsRequestWrapper parse(InputStream stream) {
    logger.debug("requestWrapper parse called.");
    UserBitShared.QueryData header = null;
    long sequenceId = -1;
    ByteBuf[] byteBuffers = null;
    CoordinationProtos.NodeEndpoint foreman = null;

    int length;
    ArrowBuf directBuf;

    try {
      while (stream.available() > 0) {
        int tag = readRawVarint32(stream);
        switch (tag) {
          case HEADER_TAG:
            header = UserBitShared.QueryData.parser().parseDelimitedFrom(stream);
            logger.debug("Parsed header:{}", header);
            break;
          case SEQUENCE_ID_TAG:
            sequenceId = readRawVarint64(stream);
            logger.debug("Parsed sequenceid:{}", sequenceId);
            break;
          case DATA_TAG:
            length = readRawVarint32(stream);
            directBuf = allocator.buffer(length);
            StreamToByteBufReader.readIntoBuffer(stream, directBuf, length);
            byteBuffers = new ByteBuf[] {NettyArrowBuf.unwrapBuffer(directBuf)};
            logger.debug("Parsed data successfully, data size:{}", length);
            break;
          case FOREMAN_TAG:
            foreman = CoordinationProtos.NodeEndpoint.parser().parseDelimitedFrom(stream);
            logger.debug("Parsed foreman:{}", foreman);
            break;
          default:
            logger.debug("Skipping unknown fields.");
            // fall - through; unknown fields;
        }
      }
      return new JobResultsRequestWrapper(header, sequenceId, byteBuffers, foreman);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static int readRawVarint32(InputStream is) throws IOException {
    int firstByte = is.read();
    if (firstByte == -1) {
      // copied from InvalidProtocolBufferException.truncatedMessage
      throw new InvalidProtocolBufferException(
          "While parsing a protocol message, the input ended unexpectedly "
              + "in the middle of a field.  This could mean either that the "
              + "input has been truncated or that an embedded message "
              + "misreported its own length.");
    }
    return CodedInputStream.readRawVarint32(firstByte, is);
  }

  // copied from CodedInputStream.StreamDecoder.readRawVarint64SlowPath()
  // because it is package protected.
  private static long readRawVarint64(InputStream is) throws IOException {
    long result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      final byte b = (byte) is.read();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint.");
  }

  @Override
  public void close() throws IOException {
    if (!inputStreams.isEmpty()) {
      for (DrainableByteBufInputStream inputStream : inputStreams) {
        inputStream.close();
      }
    }
    inputStreams.clear();
  }

  private void cleanupBefore() {
    inputStreams.removeIf(s -> s.isClosed());
  }
}
