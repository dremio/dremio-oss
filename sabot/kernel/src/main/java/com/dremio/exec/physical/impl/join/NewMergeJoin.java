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
package com.dremio.exec.physical.impl.join;

import com.dremio.sabot.op.spi.DualInputOperator;

/**
 *
 * @author jnadeau
 *
 */
public abstract class NewMergeJoin implements DualInputOperator {

//  private State state = State.NEEDS_SETUP;
//  private JoinState joinState = JoinState.INCOMPLETE_RIGHT_SEQUENCE;
//
//  enum JoinState {
//
//    // searching for sequence completion in left side.
//    INCOMPLETE_LEFT_SEQUENCE,
//
//    // searching for sequence completion in right side.
//    INCOMPLETE_RIGHT_SEQUENCE,
//
//    // within a sequence, outputting null left records.
//    MID_LEFT_OUTPUT,
//
//    // within a sequence, outputting null right records.
//    MID_RIGHT_OUTPUT,
//
//    // within a sequence, outputting inner join matching records.
//    MID_JOINT_OUTPUT
//  }
//
//  private VectorContainer outgoing;
//  private SequenceFinder leftFinder;
//  private SequenceFinder rightFinder;
//
//  private boolean leftIsComplete;
//  private boolean rightIsComplete;
//
//  private boolean leftAlwaysInclude;
//  private boolean rightAlwaysInclude;
//
//  private SequenceShuttle leftShuttle;
//  private SequenceShuttle rightShuttle;
//
//  @Override
//  public VectorAccessible setup(VectorAccessible left, VectorAccessible right) throws Exception {
//    state.is(State.NEEDS_SETUP);
//
//    // todo compile
//
//    state = State.CAN_CONSUME_R;
//    return outgoing;
//  }
//
//
//
//
//
//  @Override
//  public void noMoreToConsumeLeft() throws Exception {
//    state.is(State.CAN_CONSUME_L);
//    leftIsComplete = true;
//
//  }
//
//  @Override
//  public void noMoreToConsumeRight() throws Exception {
//    state.is(State.CAN_CONSUME_R);
//    rightIsComplete = true;
//  }
//
//  @Override
//  public void consumeDataLeft(int records) throws Exception {
//    leftShuttle.consume(records);
//
//    // if we still don't have a sequence, get the next batch.
//    if(!leftShuttle.nextSequence(true)){
//      return;
//    }
//
//    if(!rightShuttle.withinSequence()){
//      state = State.CAN_CONSUME_R;
//      return;
//    }
//
//  }
//
//  @Override
//  public State getState() {
//    return state;
//  }
//
//  @Override
//  public void close() throws Exception {
//  }
//
//  /**
//   * Copy records until completed.
//   * @return
//   */
////  private boolean copyRecordsWork(){
////    switch(joinState){
////    case MID_JOINT_OUTPUT:
////      return copyJointRecords();
////    case MID_LEFT_OUTPUT:
////      return copyLeftRecords();
////    case MID_RIGHT_OUTPUT:
////      return copyRightRecords();
////    default:
////      throw new UnsupportedOperationException();
////    }
////  }
////
////  private boolean copyLeftRecords(){
////
////  }
////  private boolean copyRightRecords(){
////
////  }
////
////  private boolean copyJointRecords(){
////
////  }
////
////  private void doJoinWork(){
////    while(true) {
////
////      int comparison = doComparison(leftShuttle.currentIndex(), rightShuttle.currentIndex());
////
////      switch(comparison){
////      case -1:
////        joinState = JoinState.MID_LEFT_OUTPUT;
////        if(leftAlwaysInclude){
////          if(!copyLeftRecords()){
////            state = State.CAN_PRODUCE;
////            return;
////          }
////        }
////
////        if(!leftShuttle.nextSequence(leftIsComplete)){
////
////        }else{
////          continue;
////        }
////
////
////        break;
////      case 0:
////        joinState = JoinState.MID_JOINT_OUTPUT;
////
////      case 1:
////        joinState = JoinState.MID_RIGHT_OUTPUT;
////      }
////    }
////  }
//
//
//
//  @Override
//  public void consumeDataRight(int records) throws Exception {
//    leftShuttle.consume(records);
//
//    // if we still don't have a sequence, get the next batch.
//    if(!rightShuttle.nextSequence(true)){
//      return;
//    }
//
//    if(!leftShuttle.withinSequence()){
//      state = State.CAN_CONSUME_R;
//      return;
//    }
//
//
//
//  }
//
//  @Override
//  public int outputData() throws Exception {
//    // TODO Auto-generated method stub
//    return 0;
//  }


}
