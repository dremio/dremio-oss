/*
 * Copyright 2012 The Netty Project
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
package io.netty.handler.proxy;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.socksx.v5.DefaultSocks5CommandResponse;
import io.netty.handler.codec.socksx.v5.DefaultSocks5InitialResponse;
import io.netty.handler.codec.socksx.v5.DefaultSocks5PasswordAuthResponse;
import io.netty.handler.codec.socksx.v5.Socks5AddressType;
import io.netty.handler.codec.socksx.v5.Socks5AuthMethod;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequest;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5CommandResponse;
import io.netty.handler.codec.socksx.v5.Socks5CommandStatus;
import io.netty.handler.codec.socksx.v5.Socks5CommandType;
import io.netty.handler.codec.socksx.v5.Socks5InitialRequest;
import io.netty.handler.codec.socksx.v5.Socks5InitialRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequest;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthStatus;
import io.netty.handler.codec.socksx.v5.Socks5ServerEncoder;
import io.netty.util.internal.SocketUtils;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/** A Socks5 proxy server used for testing client connections routed through proxy */
public final class DremioSocks5ProxyServer extends ProxyServer implements Closeable {

  private static final String ENCODER = "encoder";
  private static final String DECODER = "decoder";
  private boolean configureCalled = false;

  public DremioSocks5ProxyServer(InetSocketAddress destination) {
    super(false, TestMode.INTERMEDIARY, destination);
  }

  public DremioSocks5ProxyServer(InetSocketAddress destination, String username, String password) {
    super(false, TestMode.INTERMEDIARY, destination, username, password);
  }

  @Override
  protected void configure(SocketChannel ch) {
    ChannelPipeline p = ch.pipeline();
    p.addLast(DECODER, new Socks5InitialRequestDecoder());
    p.addLast(ENCODER, Socks5ServerEncoder.DEFAULT);
    p.addLast(new Socks5IntermediaryHandler());
    configureCalled = true;
  }

  boolean authenticate(ChannelHandlerContext ctx, Object msg) {
    if (this.username == null) {
      ctx.pipeline().replace(DECODER, DECODER, new Socks5CommandRequestDecoder());
      ctx.write(new DefaultSocks5InitialResponse(Socks5AuthMethod.NO_AUTH));
      return true;
    }

    if (msg instanceof Socks5InitialRequest) {
      ctx.pipeline().replace(DECODER, DECODER, new Socks5PasswordAuthRequestDecoder());
      ctx.write(new DefaultSocks5InitialResponse(Socks5AuthMethod.PASSWORD));
      return false;
    }

    Socks5PasswordAuthRequest req = (Socks5PasswordAuthRequest) msg;
    if (req.username().equals(this.username) && req.password().equals(this.password)) {
      ctx.pipeline().replace(DECODER, DECODER, new Socks5CommandRequestDecoder());
      ctx.write(new DefaultSocks5PasswordAuthResponse(Socks5PasswordAuthStatus.SUCCESS));
      return true;
    }

    ctx.pipeline().replace(DECODER, DECODER, new Socks5PasswordAuthRequestDecoder());
    ctx.write(new DefaultSocks5PasswordAuthResponse(Socks5PasswordAuthStatus.FAILURE));
    return false;
  }

  private final class Socks5IntermediaryHandler extends IntermediaryHandler {

    private boolean authenticated;
    private SocketAddress intermediaryDestination;

    @Override
    protected boolean handleProxyProtocol(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (!authenticated) {
        authenticated = authenticate(ctx, msg);
        return false;
      }

      Socks5CommandRequest req = (Socks5CommandRequest) msg;
      assertThat(req.type()).isEqualTo(Socks5CommandType.CONNECT);

      Socks5CommandResponse res =
          new DefaultSocks5CommandResponse(Socks5CommandStatus.SUCCESS, Socks5AddressType.IPv4);
      intermediaryDestination = SocketUtils.socketAddress(req.dstAddr(), req.dstPort());

      ctx.write(res);

      ctx.pipeline().remove(ENCODER);
      ctx.pipeline().remove(DECODER);

      return true;
    }

    @Override
    protected SocketAddress intermediaryDestination() {
      return intermediaryDestination;
    }
  }

  @Override
  public void close() {
    this.stop();
  }

  public boolean interceptedConnection() {
    return configureCalled;
  }
}
