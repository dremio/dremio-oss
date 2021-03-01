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
package com.dremio.test.redis;

import org.junit.rules.TestRule;

import redis.clients.jedis.Jedis;

/**
 * Resource to interact with a redis server
 */
public interface RedisResource extends TestRule {
  String REDIS_HOST = "localhost";
  int REDIS_PORT = 6379;

  static RedisResource newSingleRedis() {
    return new SingleRedisResource();
  }
  /**
   * Gets a new client configured to connect with the redis
   * @return jedis based redis client
   */
  Jedis newClient();

  /**
   *
   * @return Redis Client URI used to connect to redis
   */
  default String getURI() {
    return String.format("redis://%s:%s", REDIS_HOST, getPort());
  }

  /**
   * Gets the current port for the redis resource.
   */
  int getPort();
}
