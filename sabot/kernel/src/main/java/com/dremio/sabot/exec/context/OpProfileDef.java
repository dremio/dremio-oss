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
package com.dremio.sabot.exec.context;

public class OpProfileDef {

  public int operatorId;
  public int operatorType;
  public int incomingCount;

  public OpProfileDef(int operatorId, int operatorType, int incomingCount) {
    this.operatorId = operatorId;
    this.operatorType = operatorType;
    this.incomingCount = incomingCount;
  }
  public int getOperatorId(){
    return operatorId;
  }

  public int getOperatorType(){
    return operatorType;
  }
  public int getIncomingCount(){
    return incomingCount;
  }

}
