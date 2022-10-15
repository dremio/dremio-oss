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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import redis.clients.jedis.Jedis;

/**
 * Test SingleRedisResource with simple redis operations.
 * Demonstrates usage of RedisResource.newSingleRedis().
 */
public class TestSingleRedisResource {
  @ClassRule
  public static final RedisResource REDIS_RESOURCE = RedisResource.newSingleRedis();
  private Jedis redisClient;

  @Before
  public void setup() {
    redisClient = REDIS_RESOURCE.newClient();
  }

  @After
  public void cleanup() {
    redisClient.flushAll();
  }

  @Test
  public void testKeyPresence() throws Exception {
    redisClient.mset("abc", "1", "def", "2");

    assertEquals("1", redisClient.mget("abc").get(0));
    assertEquals("2", redisClient.mget("def").get(0));
  }

  @Test
  public void testKeyAbsence() throws Exception {
    assertNull(redisClient.mget("xyz").get(0));
  }

  @Test(expected =  AssertionError.class)
  public void testKeyAbsenceException() throws Exception {
    redisClient.mset("abc", "3", "xyz", "4");
    assertNull(redisClient.mget("xyz").get(0));
  }

}
