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

package com.dremio.sabot.exec.heap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.management.ObjectName;
import org.junit.Test;

public class TestSpillingOperatorHeapController {
  private static final long MAX_WIDTH = 128 * 32;
  private static final long KB = 1024;
  private static final long MB = KB * KB;
  private static final long GB = KB * MB;
  private final Random rand = new Random();

  @Test
  public void testSpillingOperatorBasicApi() {
    try (SpillingOperatorHeapController ut = new SpillingOperatorHeapController()) {
      HeapLowMemParticipant participant = ut.addParticipant("1:2", 21000);
      for (int i = 0; i < 1000; i++) {
        participant.addBatches(1000);
        assertFalse(participant.isVictim());
      }
      assertFalse(participant.isVictim());
      ut.removeParticipant("1:2", 21000);
    }
  }

  @Test
  public void testSpillingOperatorMultiParticipant() {
    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      generateAndRemove(100, 100, sut, true);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  @Test
  public void testSpillingOperatorMultiThreaded() {
    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      List<CompletableFuture<Void>> allFutures = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        allFutures.add(
            CompletableFuture.supplyAsync(
                () ->
                    generateAndRemove(
                        100 + rand.nextInt(100), 1000 + rand.nextInt(1000), sut, false)));
      }
      allFutures.forEach(CompletableFuture::join);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  @Test
  public void testNormalCondition() {
    TestMemoryPool myPool = new TestMemoryPool("My Old Gen Pool");
    myPool.setUsage(new MemoryUsage(KB, MB, MB, GB));

    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      sut.getLowMemListener().handleMemNotification(false, myPool);
      List<GeneratedParticipant> participants = generateAndAddBatches(100, 100, sut, true);
      sut.getLowMemListener().handleMemNotification(true, myPool);
      participants.forEach(GeneratedParticipant::remove);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  @Test
  public void testBorderConditionLowParticipant() throws InterruptedException {
    TestMemoryPool myPool = new TestMemoryPool("My Old Gen Pool");
    myPool.setUsage(new MemoryUsage(MB, MB, 8 * GB, 8 * GB));

    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      sut.getLowMemListener().handleMemNotification(false, myPool);
      sut.getLowMemListener().changeLowMemOptions(50, MAX_WIDTH);
      List<GeneratedParticipant> participants = generateParticipants(10, sut, 450);
      participants.forEach((x) -> x.lowMemParticipant.addBatches(2));
      myPool.setUsage(new MemoryUsage(MB, 4 * GB + 2 * MB, 8 * GB, 8 * GB));
      sut.getLowMemListener().handleUsageCrossedNotification();
      for (GeneratedParticipant p : participants) {
        assertFalse(p.lowMemParticipant.isVictim());
      }
      Thread.sleep(1000);
      for (GeneratedParticipant p : participants) {
        assertFalse(p.lowMemParticipant.isVictim());
      }
      participants.forEach(GeneratedParticipant::remove);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  @Test
  public void testBorderConditionLowWidth() throws InterruptedException {
    TestMemoryPool myPool = new TestMemoryPool("My Old Gen Pool");
    myPool.setUsage(new MemoryUsage(MB, MB, 8 * GB, 8 * GB));

    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      sut.getLowMemListener().handleMemNotification(false, myPool);
      sut.getLowMemListener().changeLowMemOptions(50, 256);
      List<GeneratedParticipant> participants = generateParticipants(100, sut, 250);
      participants.forEach(
          (x) -> x.lowMemParticipant.addBatches(MemoryState.BORDER.getIndividualOverhead() + 10));
      myPool.setUsage(new MemoryUsage(MB, 4 * GB + 2 * MB, 8 * GB, 8 * GB));
      sut.getLowMemListener().handleUsageCrossedNotification();
      for (GeneratedParticipant p : participants) {
        assertFalse(p.lowMemParticipant.isVictim());
      }
      Thread.sleep(2000);
      myPool.setUsage(new MemoryUsage(MB, MB, 8 * GB, 8 * GB));
      for (GeneratedParticipant p : participants) {
        assertFalse(p.lowMemParticipant.isVictim());
      }
      participants.forEach(GeneratedParticipant::remove);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  @Test
  public void testBorderConditionHighParticipationOne() throws InterruptedException {
    TestMemoryPool myPool = new TestMemoryPool("My Old Gen Pool");
    myPool.setUsage(new MemoryUsage(MB, MB, 8 * GB, 8 * GB));

    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      sut.getLowMemListener().handleMemNotification(false, myPool);
      sut.getLowMemListener().changeLowMemOptions(50, 128);
      int maxVictims = MemoryState.BORDER.getMaxVictims(500);
      List<GeneratedParticipant> participants = generateParticipants(maxVictims, sut, 450);
      List<GeneratedParticipant> participants2 = generateParticipants(10, sut, 220);
      List<GeneratedParticipant> participants3 =
          generateParticipants(500 - (maxVictims + 10), sut, 901);
      participants.stream()
          .filter((g) -> !g.participantId.contains("p:99:"))
          .forEach(
              (x) ->
                  x.lowMemParticipant.addBatches(MemoryState.BORDER.getIndividualOverhead() / 450));
      participants.forEach((x) -> x.lowMemParticipant.addBatches(2));
      participants.forEach((x) -> x.lowMemParticipant.addBatches(1));
      participants.stream()
          .filter((g) -> !g.participantId.contains("p:99:"))
          .forEach((x) -> x.lowMemParticipant.addBatches(100));
      participants2.forEach((x) -> x.lowMemParticipant.addBatches(2));
      participants3.forEach((x) -> x.lowMemParticipant.addBatches(2));
      myPool.setUsage(new MemoryUsage(MB, 4 * GB + 2 * MB, 8 * GB, 8 * GB));
      sut.getLowMemListener().handleUsageCrossedNotification();
      Thread.sleep(2000);
      myPool.setUsage(new MemoryUsage(MB, GB, 8 * GB, 8 * GB));
      int numVictims = 0;
      for (GeneratedParticipant p : participants) {
        if (p.lowMemParticipant.isVictim()) {
          numVictims++;
        }
      }
      assertTrue(
          "Num victims expected to be less than 100 but was " + numVictims,
          numVictims < maxVictims);
      List<GeneratedParticipant> allParticipants = new ArrayList<>(participants2);
      allParticipants.addAll(participants3);
      numVictims = 0;
      for (GeneratedParticipant p : allParticipants) {
        if (p.lowMemParticipant.isVictim()) {
          numVictims++;
        }
      }
      assertEquals(0, numVictims);
      participants.forEach(GeneratedParticipant::remove);
      allParticipants.forEach(GeneratedParticipant::remove);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  @Test
  public void testBorderConditionHighParticipationMany() throws InterruptedException {
    TestMemoryPool myPool = new TestMemoryPool("My Old Gen Pool");
    myPool.setUsage(new MemoryUsage(MB, MB, 8 * GB, 8 * GB));

    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      sut.getLowMemListener().handleMemNotification(false, myPool);
      sut.getLowMemListener().changeLowMemOptions(50, 250);
      int maxVictims = MemoryState.BORDER.getMaxVictims(750);
      List<GeneratedParticipant> participants = generateParticipants(maxVictims - 2, sut, 450);
      List<GeneratedParticipant> participants2 = generateParticipants(10, sut, 256);
      List<GeneratedParticipant> participants3 = generateParticipants(maxVictims + 10, sut, 220);
      List<GeneratedParticipant> participants4 = generateParticipants(100, sut, 901);
      participants.stream()
          .filter((g) -> !g.participantId.contains("p:99:"))
          .forEach(
              (x) ->
                  x.lowMemParticipant.addBatches(
                      (MemoryState.BORDER.getIndividualOverhead() / 450) + 100));
      participants4.stream()
          .filter((g) -> g.participantId.contains("p:99:"))
          .forEach(
              (x) ->
                  x.lowMemParticipant.addBatches(
                      (MemoryState.BORDER.getIndividualOverhead() / 901) + 100));
      participants2.forEach((x) -> x.lowMemParticipant.addBatches(2));
      participants3.forEach((x) -> x.lowMemParticipant.addBatches(200));
      myPool.setUsage(new MemoryUsage(MB, 4 * GB + 2 * MB, 8 * GB, 8 * GB));
      sut.getLowMemListener().handleUsageCrossedNotification();
      Thread.sleep(2000);
      myPool.setUsage(new MemoryUsage(MB, GB, 8 * GB, 8 * GB));
      int numVictims = 0;
      for (GeneratedParticipant p : participants) {
        if (p.lowMemParticipant.isVictim()) {
          numVictims++;
        }
      }
      assertTrue(
          "Num victims expected to be less than " + maxVictims + " but was " + numVictims,
          numVictims < maxVictims);
      numVictims = 0;
      for (GeneratedParticipant p : participants4) {
        if (p.lowMemParticipant.isVictim()) {
          numVictims++;
        }
      }
      assertTrue("Num victims expected to be less than 2 but was " + numVictims, numVictims < 2);
      List<GeneratedParticipant> allParticipants = new ArrayList<>(participants2);
      allParticipants.addAll(participants3);
      numVictims = 0;
      for (GeneratedParticipant p : allParticipants) {
        if (p.lowMemParticipant.isVictim()) {
          numVictims++;
        }
      }
      assertEquals(0, numVictims);
      participants.forEach(GeneratedParticipant::remove);
      participants4.forEach(GeneratedParticipant::remove);
      allParticipants.forEach(GeneratedParticipant::remove);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  @Test
  public void testLowCondition() throws Exception {
    TestMemoryPool myPool = new TestMemoryPool("My Old Gen Pool");
    myPool.setUsage(new MemoryUsage(MB, MB, 8 * GB, 8 * GB));

    try (SpillingOperatorHeapController sut = new SpillingOperatorHeapController()) {
      sut.getLowMemListener().handleMemNotification(false, myPool);
      sut.getLowMemListener().changeLowMemOptions(50, MAX_WIDTH);
      List<GeneratedParticipant> participants = generateParticipants(10, sut, 450);
      List<GeneratedParticipant> participants2 = generateParticipants(10, sut, 220);
      List<GeneratedParticipant> participants3 = generateParticipants(10, sut, 901);
      participants.forEach((x) -> x.lowMemParticipant.addBatches(100));
      participants.forEach((x) -> x.lowMemParticipant.addBatches(2));
      participants2.forEach((x) -> x.lowMemParticipant.addBatches(1000));
      participants2.forEach((x) -> x.lowMemParticipant.addBatches(200));
      participants3.forEach((x) -> x.lowMemParticipant.addBatches(100));
      myPool.setUsage(new MemoryUsage(MB, 5 * GB + 2 * MB, 8 * GB, 8 * GB));
      sut.getLowMemListener().handleUsageCrossedNotification();
      Thread.sleep(2000);
      myPool.setUsage(new MemoryUsage(MB, GB, 8 * GB, 8 * GB));
      List<GeneratedParticipant> allParticipants = new ArrayList<>(participants);
      allParticipants.addAll(participants2);
      allParticipants.addAll(participants3);
      int numVictims = 0;
      for (GeneratedParticipant p : allParticipants) {
        if (p.lowMemParticipant.isVictim()) {
          numVictims++;
        }
      }
      assertEquals(30, numVictims);
      allParticipants.forEach(GeneratedParticipant::remove);
      assertEquals(0, sut.maxParticipantsPerSlot());
      assertEquals(0, sut.numParticipants());
      assertEquals(0, sut.computeTotalOverhead());
    }
  }

  private Void generateAndRemove(
      int participantCount, int loopCount, SpillingOperatorHeapController sut, boolean single) {
    final List<GeneratedParticipant> participants =
        generateAndAddBatches(participantCount, loopCount, sut, single);
    participants.forEach(GeneratedParticipant::remove);
    return null;
  }

  private List<GeneratedParticipant> generateAndAddBatches(
      int participantCount, int loopCount, SpillingOperatorHeapController sut, boolean single) {
    List<GeneratedParticipant> generatedParticipants = generateParticipants(participantCount, sut);
    if (single) {
      // if single threaded, check if participant count is spread
      assertTrue(sut.maxParticipantsPerSlot() < participantCount / 2);
    }
    for (int i = 0; i < loopCount; i++) {
      int idx = rand.nextInt(participantCount);
      HeapLowMemParticipant p1 = generatedParticipants.get(idx).lowMemParticipant;
      p1.addBatches(1 + rand.nextInt(2));
      assertFalse(p1.isVictim());
    }
    return generatedParticipants;
  }

  private List<GeneratedParticipant> generateParticipants(
      int participantCount, SpillingOperatorHeapController sut) {
    List<GeneratedParticipant> participants = new ArrayList<>();
    for (int i = 0; i < participantCount; i++) {
      participants.add(new GeneratedParticipant(i + 1, rand, sut));
    }
    return participants;
  }

  private List<GeneratedParticipant> generateParticipants(
      int participantCount, SpillingOperatorHeapController sut, int fieldOverhead) {
    List<GeneratedParticipant> participants = new ArrayList<>();
    for (int i = 0; i < participantCount; i++) {
      participants.add(new GeneratedParticipant(i + 1, fieldOverhead, sut));
    }
    return participants;
  }

  /**
   * Mocking MemoryPool causes random issues in multi-threaded environment. Implement a Test memory
   * pool to avoid this
   */
  private static final class TestMemoryPool implements MemoryPoolMXBean {
    private final String name;
    private volatile MemoryUsage usage;

    TestMemoryPool(String name) {
      this.name = name;
    }

    private void setUsage(MemoryUsage usage) {
      this.usage = usage;
    }

    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public MemoryType getType() {
      return MemoryType.HEAP;
    }

    @Override
    public MemoryUsage getUsage() {
      return usage;
    }

    @Override
    public MemoryUsage getPeakUsage() {
      return usage;
    }

    @Override
    public void resetPeakUsage() {}

    @Override
    public boolean isValid() {
      return true;
    }

    @Override
    public String[] getMemoryManagerNames() {
      return new String[0];
    }

    @Override
    public long getUsageThreshold() {
      return 0;
    }

    @Override
    public void setUsageThreshold(long threshold) {}

    @Override
    public boolean isUsageThresholdExceeded() {
      return false;
    }

    @Override
    public long getUsageThresholdCount() {
      return 0;
    }

    @Override
    public boolean isUsageThresholdSupported() {
      return true;
    }

    @Override
    public long getCollectionUsageThreshold() {
      return 0;
    }

    @Override
    public void setCollectionUsageThreshold(long threshold) {}

    @Override
    public boolean isCollectionUsageThresholdExceeded() {
      return true;
    }

    @Override
    public long getCollectionUsageThresholdCount() {
      return 0;
    }

    @Override
    public MemoryUsage getCollectionUsage() {
      return null;
    }

    @Override
    public boolean isCollectionUsageThresholdSupported() {
      return false;
    }

    @Override
    public ObjectName getObjectName() {
      return null;
    }
  }

  private static final class GeneratedParticipant {
    private final String participantId;
    private final int fieldOverhead;
    private final HeapLowMemParticipant lowMemParticipant;
    private final SpillingOperatorHeapController controller;

    private GeneratedParticipant(int id, Random rand, SpillingOperatorHeapController cut) {
      this.participantId = "p:" + id + ":" + UUID.randomUUID();
      this.fieldOverhead = rand.nextInt(128) * 32 + rand.nextInt(32);
      this.controller = cut;
      this.lowMemParticipant = this.controller.addParticipant(participantId, fieldOverhead);
    }

    private GeneratedParticipant(int id, int fieldOverhead, SpillingOperatorHeapController cut) {
      this.participantId = "p:" + id + ":" + UUID.randomUUID();
      this.fieldOverhead = fieldOverhead;
      this.controller = cut;
      this.lowMemParticipant = this.controller.addParticipant(participantId, fieldOverhead);
    }

    private void remove() {
      this.controller.removeParticipant(participantId, fieldOverhead);
    }
  }
}
