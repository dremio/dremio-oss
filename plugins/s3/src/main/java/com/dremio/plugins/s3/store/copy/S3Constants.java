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
package com.dremio.plugins.s3.store.copy;

/**
 * (Copied from Hadoop 2.8.3, move to using Hadoop version once hadoop version in MapR profile is upgraded to 2.8.3+)
 */
public class S3Constants {
  // socket send buffer to be used in Amazon client
  public static final String SOCKET_SEND_BUFFER = "fs.s3a.socket.send.buffer";
  public static final int DEFAULT_SOCKET_SEND_BUFFER = 8 * 1024;

  // socket send buffer to be used in Amazon client
  public static final String SOCKET_RECV_BUFFER = "fs.s3a.socket.recv.buffer";
  public static final int DEFAULT_SOCKET_RECV_BUFFER = 8 * 1024;

  public static final String SIGNING_ALGORITHM = "fs.s3a.signing-algorithm";

  public static final String USER_AGENT_PREFIX = "fs.s3a.user.agent.prefix";

  public static final String FAST_UPLOAD_BUFFER = "fs.s3a.fast.upload.buffer";
  public static final String FAST_UPLOAD_ACTIVE_BLOCKS = "fs.s3a.fast.upload.active.blocks";

  //Enable path style access? Overrides default virtual hosting
  public static final String PATH_STYLE_ACCESS = "fs.s3a.path.style.access";

  // aws credentials provider
  public static final String AWS_CREDENTIALS_PROVIDER =
      "fs.s3a.aws.credentials.provider";
}
