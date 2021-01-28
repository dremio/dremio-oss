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
package com.dremio.exec.rpc;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Locale;
import java.util.concurrent.ThreadFactory;

import com.dremio.common.concurrent.NamedThreadFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.SystemPropertyUtil;

/**
 * TransportCheck decides whether or not to use the native EPOLL mechanism for communication.
 */
public final class TransportCheck {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TransportCheck.class);

  private static final String USE_LINUX_EPOLL = "dremio.exec.enable-epoll";

  public static final boolean SUPPORTS_EPOLL;

  static{

    String name = SystemPropertyUtil.get("os.name").toLowerCase(Locale.US).trim();

    if (name.startsWith("linux") && SystemPropertyUtil.getBoolean(USE_LINUX_EPOLL, false)) {
      SUPPORTS_EPOLL = true;
    } else {
      SUPPORTS_EPOLL = false;
    }
  }

  private TransportCheck() {}

  public static Class<? extends ServerSocketChannel> getServerSocketChannel(){
    if(SUPPORTS_EPOLL){
      return EpollServerSocketChannel.class;
    }else{
      return NioServerSocketChannel.class;
    }
  }

  public static Class<? extends SocketChannel> getClientSocketChannel(){
    if(SUPPORTS_EPOLL){
      return EpollSocketChannel.class;
    }else{
      return NioSocketChannel.class;
    }
  }

  public static EventLoopGroup createEventLoopGroup(int nThreads, String prefix) {
     if(SUPPORTS_EPOLL){
       return new EpollEventLoopGroup(nThreads, newThreadFactory(prefix));
     }else{
       return new NioEventLoopGroup(nThreads, newThreadFactory(prefix));
     }
  }

  private static final UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = (thread, t) -> logger.error("Uncaught exception in thread {}", thread.getName(), t);
  public static ThreadFactory newThreadFactory(String prefix) {
    final ThreadFactory namedThreadFactory = new NamedThreadFactory(prefix);

    // Adding an uncaught exception handler to make sure threads are logged using slf4j
    return (runnable) -> {
      final Thread result = namedThreadFactory.newThread(runnable);
      result.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);

      return result;
    };
  }
}
