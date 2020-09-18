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
package com.dremio.service.reflection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalHash;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.collect.ImmutableList;

/**
 * Test functionality on the ReflectionGoalsStore.
 */
public class TestReflectionGoalChecker {

  @Test
  public void testMatchesTag() {
    final String testTag = "testTag";
    final ReflectionGoal goal = ReflectionGoal.getDefaultInstance().newMessage()
      .setTag(testTag);
    assertTrue(ReflectionGoalChecker.checkGoal(goal, new ReflectionEntry().setGoalVersion(testTag)));
  }

  @Test
  public void testMatchesVersion() {
    final String testTag = "1";
    final ReflectionGoal goal = ReflectionGoal.getDefaultInstance().newMessage()
      .setTag("wrongTag")
      .setVersion(Long.valueOf(testTag));
    assertTrue(ReflectionGoalChecker.checkGoal(goal, new ReflectionEntry().setGoalVersion(testTag)));
  }

  @Test
  public void testMismatchNonNullVersion() {
    final String testTag = "1";
    final ReflectionGoal goal = ReflectionGoal.getDefaultInstance().newMessage()
      .setTag("wrongTag")
      .setVersion(0L);
    assertFalse(ReflectionGoalChecker.checkGoal(goal, new ReflectionEntry().setGoalVersion(testTag)));
  }

  @Test
  public void testMismatchNullVersion() {
    final String testTag = "1";
    final ReflectionGoal goal = ReflectionGoal.getDefaultInstance().newMessage()
      .setTag("wrongTag")
      .setVersion(null);
    assertFalse(ReflectionGoalChecker.checkGoal(goal, new ReflectionEntry().setGoalVersion(testTag)));
  }

  @Test
  public void testCheckHashAgainstHash(){
    ReflectionGoal goal = new ReflectionGoal();
    ReflectionEntry entry = new ReflectionEntry()
      .setReflectionGoalHash(new ReflectionGoalHash("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));

    assertTrue(ReflectionGoalChecker.Instance.checkHash(goal, entry));
  }

  @Test
  public void testCheckHashAgainstNullHash(){
    ReflectionGoal goal = new ReflectionGoal();
    ReflectionEntry entry = new ReflectionEntry();

    assertFalse(ReflectionGoalChecker.Instance.checkHash(goal, entry));
  }

  @Test
  public void testHashingEmptyGoal(){
   ReflectionGoal goal = new ReflectionGoal();
   ReflectionGoalHash actual = ReflectionGoalChecker.Instance.calculateReflectionGoalVersion(goal);
   assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", actual.getHash());
  }


  @Test
  public void testHashingEmptyIfExcludingBlackListFieldsGoal(){
    ReflectionGoal goal = new ReflectionGoal()
      .setTag(Long.toString(System.currentTimeMillis()))
      .setCreatedAt(System.currentTimeMillis())
      .setModifiedAt(System.currentTimeMillis())
      .setVersion(System.currentTimeMillis())
      .setName("Name")
      .setArrowCachingEnabled(false);

    ReflectionGoalHash actual = ReflectionGoalChecker.Instance.calculateReflectionGoalVersion(goal);
    assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", actual.getHash());
  }

  @Test
  public void testHashingEmptyWithBoostEnabledGoal(){
    ReflectionGoal goal = new ReflectionGoal()
      .setArrowCachingEnabled(true);

    ReflectionGoalHash actual = ReflectionGoalChecker.Instance.calculateReflectionGoalVersion(goal);
    assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", actual.getHash());
  }

  @Test
  public void testHashingWithUniqueIdGoal(){
    ReflectionGoal goal = new ReflectionGoal()
      .setId(new ReflectionId("xxx"));

    ReflectionGoalHash actual = ReflectionGoalChecker.Instance.calculateReflectionGoalVersion(goal);
    assertEquals("a9171a2d1e967b50401388ba1271fba59386adac44078e0a0047f9111167f1a2", actual.getHash());
  }

  @Test
  public void testHashingWithDifferentDetails(){
    ReflectionGoal goal1 = new ReflectionGoal()
      .setId(new ReflectionId("xxx"))
      .setDetails(
        new ReflectionDetails()
          .setDimensionFieldList(ImmutableList.of(
              new ReflectionDimensionField()
                .setName("field")
                .setGranularity(DimensionGranularity.NORMAL)
            ))
      );

    ReflectionGoal goal2 = new ReflectionGoal()
      .setId(new ReflectionId("xxx"))
      .setDetails(
        new ReflectionDetails()
          .setDimensionFieldList(ImmutableList.of(
            new ReflectionDimensionField()
              .setName("field2")
              .setGranularity(DimensionGranularity.DATE)
          ))
      );

    ReflectionGoalHash actual1 = ReflectionGoalChecker.Instance.calculateReflectionGoalVersion(goal1);
    ReflectionGoalHash actual2 = ReflectionGoalChecker.Instance.calculateReflectionGoalVersion(goal2);

    assertEquals(
      "We expect difrrent values for different details",
      "c5f4f595fb56e276b02a2e6fa6cb7fad1ca2afab691fee3a7e0465fc93fb1607",
      actual1.getHash()
    );
    assertEquals(
      "We expect difrrent values for different details",
      "296a48c3f656ad3430da74353556d40fe4d0756952e2ce80cd7e6df58b064275",
      actual2.getHash()
    );
  }
}
