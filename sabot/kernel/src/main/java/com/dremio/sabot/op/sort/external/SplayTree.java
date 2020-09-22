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
package com.dremio.sabot.op.sort.external;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

import org.apache.arrow.memory.ArrowBuf;

/**
 * SplayTree held in a single block of memory. Structure is follows:
 *
 * [ 0000 0000 0000 0000 ] [ aaaa bbbb cccc dddd ] [ eeee ffff gggg hhhh ] ...
 *
 * first 16 bytes are empty. This is because we use 0 as the NULL position.
 *
 * aaaa - dddd: information for the first node in the tree.
 * eeee - hhhh: information for the second node in the tree.
 * aaaa, eeee: the node value (0 byte offset)
 * bbbb, ffff: the node left position (4 byte offset)
 * cccc, gggg: the node right position (8 byte offset)
 * dddd, hhhh: the node parent position. (12 byte offset)
 */
public abstract class SplayTree {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplayTree.class);

  private static final int NULL = 0;

  private static final int OFFSET_VALUE = 0;
  private static final int OFFSET_LEFT = 4;
  private static final int OFFSET_RIGHT = 8;
  private static final int OFFSET_PARENT = 12;
  public static final int NODE_SIZE = 16;

  private ArrowBuf data;
  private int root = NULL;
  private int totalCount;

  /**
   * Uses memory but doesn't manage it.
   *
   * @param data
   *          The buffer to use for data. Note that this needs to resized
   *          externally if you want to grow the tree. The internal logic
   *          assumes this is the right size.
   */
  public void setData(ArrowBuf data){
    this.data = data;
  }

  public SplayIterator iterator(){
    return new SplayIterator();
  }

  public int compare(int leftIndex, int rightIndex) {
    final int val1 = data.getInt(leftIndex * NODE_SIZE);
    final int val2 = data.getInt(rightIndex * NODE_SIZE);
    return compareValues(val1, val2);
  }

  public int getTotalCount(){
    return totalCount;
  }

  public abstract int compareValues(int leftVal, int rightVal);

  public void put(final int newIndex) {
    // start all node numbers from 1 so that zero can be the null value.
    final int n = ++totalCount;

    // create new node struct (insert index in value offset).
    data.setInt(n * NODE_SIZE, newIndex);

    int c = root;

    if (c == NULL) {
      root = n;
      return;
    }

    while (true) {

      final int cmp = compare(n, c);
      // System.out.printf("compare %d %d = %d\n", n, c, cmp);
      if (cmp == 0) {
        setLeft(n, getLeft(c));
        // System.out.printf("set %d.left %d\n", n, getLeft(n));

        setLeft(c, n);
        // System.out.printf("set %d.left %d\n", c, getLeft(c));
        break;

      } else if (cmp < 0) {
        final int l = getLeft(c);
        // System.out.printf("%d.left = %d\n", c, l);
        if (l == NULL) {
          setLeft(c, n);
          // System.out.printf("set %d.left %d\n", c, getLeft(c));
          c = n;
          break;
        } else {
          c = l;
        }
      } else {
        final int r = getRight(c);
        // System.out.printf("%d.right = %d\n", c, r);
        if (r == NULL) {
          setRight(c, n);
          // System.out.printf("set %d.right %d\n", c, getRight(c));
          c = n;
          break;
        } else {
          c = r;
        }
      }
    }

    // validate();
    // System.out.println("before splay");
    // print();

    splay(c);
  }

  private void validate() {
    Queue<Integer> q = new LinkedList<Integer>();
    Set<Integer> process = new HashSet<>();
    q.add(root);
    Integer c;
    while ((c = q.poll()) != null) {
      int l = getLeft(c);
      if (l != NULL) {
        q.add(l);
      }
      if (process.contains(c)) {
        // System.out.println("invalid tree");
        print();
        throw new RuntimeException();
      }
      process.add(c);
      int r = getRight(c);
      if (r != NULL) {
        q.add(getRight(c));
      }
    }
  }

  private void splay(final int n) {
    // System.out.printf("splay %d\n", n);
    while (true) {
      if (n == root) {
        return;
      }
      if (getParent(n) == root) {
        // System.out.printf("%d.parent = %d (root)\n", n, root);
        if (isLeft(root, n)) {
          // System.out.printf("rotate right %d\n", n);
          rotateRight(root);
        } else {
          // System.out.printf("rotate left %d\n", n);
          rotateLeft(root);
        }
        root = n;
        setParent(root, NULL);
        return;
      }

      final int p = getParent(n);
      final int g = getParent(p);

      if (g == root) {
        root = n;
        setParent(root, NULL);
      }

      assert n != NULL : "n null";
      assert p != NULL : "p null";
      assert g != NULL : "g null";

      if (isLeft(p, n)) {
        if (isLeft(g, p)) {
          // System.out.printf("rotate right %d\n", g);
          rotateRight(g);
          // System.out.println("after 1 splay iteration");
          // print();
          // System.out.printf("rotate right %d\n", p);
          rotateRight(p);
        } else {
          // System.out.printf("rotate right %d\n", p);
          rotateRight(p);
          // System.out.println("after 1 splay iteration");
          // print();
          // System.out.printf("rotate left %d\n", g);
          rotateLeft(g);
        }
      } else {
        if (isLeft(g, p)) {
          // System.out.printf("rotate left %d\n", p);
          rotateLeft(p);
          // System.out.println("after 1 splay iteration");
          // print();
          // System.out.printf("rotate right %d\n", g);
          rotateRight(g);
        } else {
          // System.out.printf("rotate left %d\n", g);
          rotateLeft(g);
          // System.out.println("after 1 splay iteration");
          // print();
          // System.out.printf("rotate left %d\n", p);
          rotateLeft(p);
        }
      }
      // validate();
      // System.out.println("after 1 splay iteration");
      // print();
    }
  }

  private int getLeft(int key) {
    return data.getInt(key * NODE_SIZE + OFFSET_LEFT);
  }

  private void setLeft(int key, int value) {
    // System.out.printf("set %d.left %d\n", key, value);
    data.setInt(key * NODE_SIZE + OFFSET_LEFT, value);
    if (value != NULL) {
      setParent(value, key);
    }
  }

  private boolean isLeft(int key, int child) {
    return getLeft(key) == child;
  }

  private int getRight(int key) {
    return data.getInt(key * NODE_SIZE + OFFSET_RIGHT);
  }

  private void setRight(int key, int value) {
    data.setInt(key * NODE_SIZE + OFFSET_RIGHT, value);
    if (value != NULL) {
      setParent(value, key);
    }
  }

  private boolean isRight(int key, int child) {
    return getRight(key) == child;
  }

  private int getParent(int key) {
    return data.getInt(key * NODE_SIZE + OFFSET_PARENT);
  }

  private void setParent(int key, int value) {
    data.setInt(key * NODE_SIZE + OFFSET_PARENT, value);
  }

  // right rotate
  private int rotateRight(final int h) {
    int p = getParent(h);
    int x = getLeft(h);
    setLeft(h, getRight(x));
    if (p != NULL) {
      boolean isLeft = isLeft(p, h);
      if (isLeft) {
        setLeft(p, x);
      } else {
        setRight(p, x);
      }
    } else {
      setParent(x, p);
    }
    setRight(x, h);
    return x;
  }

  // left rotate
  private int rotateLeft(final int h) {
    int p = getParent(h);
    int x = getRight(h);
    setRight(h, getLeft(x));
    if (p != NULL) {
      boolean isLeft = isLeft(p, h);
      if (isLeft) {
        setLeft(p, x);
      } else {
        setRight(p, x);
      }
    } else {
      setParent(x, p);
    }
    setLeft(x, h);
    return x;
  }

  private void print() {
    StringBuilder b = new StringBuilder();
    b.append("root: " + root + "\n");
    for (int i = 0; i < totalCount; i++) {
      b.append(i + "\t");
      b.append(data.getInt(i*NODE_SIZE) + "\t");
      b.append(data.getInt(i*NODE_SIZE + OFFSET_PARENT) + "\t");
      b.append(data.getInt(i*NODE_SIZE + OFFSET_LEFT) + "\t");
      b.append(data.getInt(i*NODE_SIZE + OFFSET_RIGHT) + "\t");
      b.append("\n");
    }
    // System.out.println(b);
  }

  public void clearData(){
    data.clear();
  }

  /**
   * Iterate through tree. Doesn't implement base iterator since that would require auotboxing int values.
   */
  public class SplayIterator {

    private int next;

    SplayIterator() {
      int i = 0;
      next = root;
      if (next == NULL) {
        return;
      }
      while (getLeft(next) != NULL) {
        i++;
//        System.out.println(i + ": " + next);

        next = getLeft(next);
      }
    }

    public boolean hasNext() {
      return next != NULL;
    }


    public int next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int r = next;
      // if you can walk right, walk right, then fully left.
      // otherwise, walk up until you come from left.
      if (getRight(next) != NULL) {
        if (getParent(getRight(next)) == NULL) {
          setParent(getRight(next), next);
        }
        next = getRight(next);
        while (getLeft(next) != NULL) {
          if (getParent(getLeft(next)) == NULL) {
            setParent(getLeft(next), next);
          }
          next = getLeft(next);
        }
        return data.getInt(r * NODE_SIZE);
      } else {
        while (true) {
          if (getParent(next) == NULL) {
            next = NULL;
            return data.getInt(r * NODE_SIZE);
          }
          if (getLeft(getParent(next)) == next) {
            next = getParent(next);
            return data.getInt(r * NODE_SIZE);
          }
          next = getParent(next);
        }
      }
    }

  }


}
