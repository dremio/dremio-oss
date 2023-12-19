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
package com.dremio.dac.model.job.diagnostics;

import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.OperatorData;
import com.dremio.service.jobAnalysis.proto.PhaseData;

/***
 * A set of methods to analyze query profile
 */
public class ProfileObservationUtil {

  private static boolean skipObservations(UserBitShared.QueryProfile profile){
    //skip observation for queries less than 5 seconds
    return !profile.hasStart() || !profile.hasEnd() || profile.getEnd()- profile.getStart() < 5000;
  }

  public static List<OperatorData.OperatorObservationType> getNotableObservationsAtOperatorLevel(UserBitShared.QueryProfile profile, int phaseId, int operatorId){
    List<OperatorData.OperatorObservationType> notableObservationAtOperatorLevelList = new ArrayList<>();

    if(skipObservations(profile)){
      return notableObservationAtOperatorLevelList;
    }

    // profile start time and end time are in milliseconds. we need nanoseconds here for operator metrics.
    long runtime = (profile.getEnd()-profile.getStart())*1000000;

    UserBitShared.MajorFragmentProfile majorFragmentProfile = ProfileObservationUtil.getPhaseDetails(profile,phaseId);

    if(ProfileObservationUtil.hasHighOperatorSpan(runtime, majorFragmentProfile, operatorId)){
      notableObservationAtOperatorLevelList.add(OperatorData.OperatorObservationType.HIGH_OPERATOR_SPAN);
    }
    if(ProfileObservationUtil.hasHighOperatorOverhead(majorFragmentProfile, operatorId)){
      notableObservationAtOperatorLevelList.add(OperatorData.OperatorObservationType.HIGH_OPERATOR_OVERHEAD);
    }
    return notableObservationAtOperatorLevelList;
  }

  public static List<PhaseData.PhaseObservationType> getNotableObservationAtPhaseLevel(UserBitShared.MajorFragmentProfile major, UserBitShared.QueryProfile profile){
    List<PhaseData.PhaseObservationType> notableObservationAtPhaseLevelList = new ArrayList<>();
    //skip observation for queries less than 5 seconds
    if(skipObservations(profile)){
      return notableObservationAtPhaseLevelList;
    }

    long profileRuntime = profile.getEnd()-profile.getStart();
    if(ProfileObservationUtil.hasBlockedTimeOnDownstreamHelper(major, profileRuntime)){
      notableObservationAtPhaseLevelList.add(PhaseData.PhaseObservationType.HIGH_BLOCKED_TIME_ON_DOWNSTREAM);
    }

    if(ProfileObservationUtil.hasHighSleepDurationHelper(major, profileRuntime)){
      notableObservationAtPhaseLevelList.add(PhaseData.PhaseObservationType.HIGH_SLEEP_DURATION);
    }

    if(ProfileObservationUtil.hasHighQueueLoadHelper(major)){
      notableObservationAtPhaseLevelList.add(PhaseData.PhaseObservationType.HIGH_QUEUE_LOAD);
    }
    return notableObservationAtPhaseLevelList;
  }

  /***
   *
   * @param profile
   * @param major
   * @param operatorId
   * @return whether the operator takes more than 20% of total query execution time.
   */
  public static boolean hasHighOperatorSpan(long runtime, UserBitShared.MajorFragmentProfile major, int operatorId) {

    for(UserBitShared.MinorFragmentProfile minor:  major.getMinorFragmentProfileList()){
      for(UserBitShared.OperatorProfile operatorProfile: minor.getOperatorProfileList()){
        if (operatorProfile.getOperatorId() == operatorId) {
          long operatorSpan=0;
          if (operatorProfile.hasSetupNanos()){
            operatorSpan+=operatorProfile.getSetupNanos();
          }
          if (operatorProfile.hasProcessNanos()){
            operatorSpan+=operatorProfile.getProcessNanos();
          }
          if (operatorProfile.hasWaitNanos()){
            operatorSpan+=operatorProfile.getWaitNanos();
          }
          if(operatorSpan > 0.2 * runtime){
            return true;
          }
        }

      }
    }
    return false;
  }

  /***
   *
   * @param major
   * @param operatorId
   * @return whether the setup time is larger than the process time given an operator within a major fragment
   */
  public static boolean hasHighOperatorOverhead(UserBitShared.MajorFragmentProfile major, int operatorId) {

    for(UserBitShared.MinorFragmentProfile minor:  major.getMinorFragmentProfileList()){
      for(UserBitShared.OperatorProfile operatorProfile: minor.getOperatorProfileList()){
        if (operatorProfile.getOperatorId() == operatorId) {
          if (operatorProfile.hasSetupNanos() && operatorProfile.hasProcessNanos()){
            if(operatorProfile.getSetupNanos()>=operatorProfile.getProcessNanos()){
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /***
   *
   * @param majorFragmentProfile
   * @return whether there is high run queue load compared to number of slices within any minor fragment given a major fragment
   */
  public static boolean hasHighQueueLoadHelper(UserBitShared.MajorFragmentProfile majorFragmentProfile){
    for(UserBitShared.MinorFragmentProfile minorFragmentProfile: majorFragmentProfile.getMinorFragmentProfileList()){
      if(minorFragmentProfile.hasRunQLoad() && minorFragmentProfile.hasNumSlices()){
        if(1.0 * minorFragmentProfile.getRunQLoad() / minorFragmentProfile.getNumSlices() > 1.5){
          return true;
        }
      }
    }
    return false;
  }

  /***
   *
   * @param majorFragmentProfile
   * @param runtime
   * @return whether there is high sleep duration in any minor fragment given a major fragment
   */
  public static boolean hasHighSleepDurationHelper(UserBitShared.MajorFragmentProfile majorFragmentProfile, long runtime){

    for(UserBitShared.MinorFragmentProfile minorFragmentProfile: majorFragmentProfile.getMinorFragmentProfileList()){
      if(minorFragmentProfile.hasSleepingDuration()){
        if(minorFragmentProfile.getSleepingDuration() > 0.2 * runtime){
          return true;
        }
      }
    }
    return false;
  }

  /***
   *
   * @param majorFragmentProfile
   * @param runtime
   * @return whether there is blocked time in downstream operator in any minor fragment given a major fragment
   */
  public static boolean hasBlockedTimeOnDownstreamHelper(UserBitShared.MajorFragmentProfile majorFragmentProfile,long runtime){
    for(UserBitShared.MinorFragmentProfile minorFragmentProfile: majorFragmentProfile.getMinorFragmentProfileList()){
      if(minorFragmentProfile.hasBlockedOnDownstreamDuration()){
        if(minorFragmentProfile.getBlockedOnDownstreamDuration() > 0.2 * runtime ){
          return true;
        }
      }
    }
    return false;
  }

  /**
   * This Method Will return Fragment information from Profile Object for requested phase Id
   */
  public static UserBitShared.MajorFragmentProfile getPhaseDetails(UserBitShared.QueryProfile profile, int phaseId) {
    UserBitShared.MajorFragmentProfile major;
    try {
      major = profile.getFragmentProfile(phaseId);
    } catch (Exception ex) {
      throw new IllegalArgumentException("Phase Id : " + phaseId + " does not Exists in Profile");
    }
    return major;
  }
}
