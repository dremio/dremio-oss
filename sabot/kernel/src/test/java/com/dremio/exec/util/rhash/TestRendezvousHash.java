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
package com.dremio.exec.util.rhash;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.hash.AlwaysOneHashFunction;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;

/**
 * Test the rendezvous hash function
 */
@SuppressWarnings("serial")
public class TestRendezvousHash {
  private static final String NODE = "node";
  private static final Random rand = new Random();
  private static final HashFunction hfunc = Hashing.murmur3_128();
  private static final Funnel<String> strFunnel = new Funnel<String>(){
    @Override
    public void funnel(String from, PrimitiveSink into) {
      into.putBytes(from.getBytes());
    }};

  @Test
  public void testEmpty() {
    RendezvousHash<String, String> h = genEmpty();
    Assert.assertEquals(null, h.get("key"));
  }

  /**
   * Ensure the same node returned for same key after a large change to the pool of nodes
   */
  @Test
  public void testConsistentAfterRemove() {
    RendezvousHash<String, String> h = genEmpty();
    for(int i = 0 ; i < 1000; i++) {
      h.add(NODE + i);
    }
    String node = h.get("key");
    Assert.assertEquals(node, h.get("key"));

    for(int i = 0; i < 250; i++) {
      String toRemove = "node" + rand.nextInt(1000);
      if(!toRemove.equals(node)) {
        h.remove(toRemove);
      }
    }
    Assert.assertEquals(node, h.get("key"));
  }

  /**
   * Ensure that a new node returned after deleted
   */
  @Test
  public void testPreviousDeleted() {
    RendezvousHash<String, String> h = genEmpty();
    h.add(NODE + 1);
    h.add(NODE + 2);
    String node = h.get("key");
    h.remove(node);
    Assert.assertTrue(Sets.newHashSet("node1", "node2").contains(h.get("key")));
    Assert.assertTrue(!node.equals(h.get("key")));
  }

  /**
   * Ensure same node will still be returned if removed/readded
   */
  @Test
  public void testReAdd() {
    RendezvousHash<String, String> h = genEmpty();
    h.add(NODE + 1);
    h.add(NODE + 2);
    String node = h.get("key");
    h.remove(node);
    h.add(node);
    Assert.assertEquals(node, h.get("key"));
  }

  /**
   * Ensure 2 hashes if have nodes added in different order will have same results
   */
  @Test
  public void testDifferentOrder() {
    RendezvousHash<String, String> h = genEmpty();
    RendezvousHash<String, String> h2 = genEmpty();
    for(int i = 0 ; i < 1000; i++) {
      h.add(NODE + i);
    }
    for(int i = 1000 ; i >= 0; i--) {
      h2.add(NODE + i);
    }
    Assert.assertEquals(h2.get("key"), h.get("key"));
  }

  @Test
  public void testCollsion() {
    HashFunction hfunc = new AlwaysOneHashFunction();
    RendezvousHash h1 = new RendezvousHash<String, String>(hfunc, strFunnel, strFunnel, new ArrayList<String>());
    RendezvousHash h2 = new RendezvousHash<String, String>(hfunc, strFunnel, strFunnel, new ArrayList<String>());

    for(int i = 0 ; i < 1000; i++) {
      h1.add(NODE + i);
    }
    for(int i = 1000 ; i >= 0; i--) {
      h2.add(NODE + i);
    }
    Assert.assertEquals(h2.get("key"), h1.get("key"));
  }

  private static RendezvousHash<String, String> genEmpty() {
    return new RendezvousHash<>(hfunc, strFunnel, strFunnel, new ArrayList<>());
  }
}
