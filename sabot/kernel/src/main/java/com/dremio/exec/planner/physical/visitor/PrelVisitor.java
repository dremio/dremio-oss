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
package com.dremio.exec.planner.physical.visitor;

import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.WriterPrel;


public interface PrelVisitor<RETURN, EXTRA, EXCEP extends Throwable> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PrelVisitor.class);

  public RETURN visitExchange(ExchangePrel prel, EXTRA value) throws EXCEP;
  public RETURN visitScreen(ScreenPrel prel, EXTRA value) throws EXCEP;
  public RETURN visitWriter(WriterPrel prel, EXTRA value) throws EXCEP;
  public RETURN visitLeaf(LeafPrel prel, EXTRA value) throws EXCEP;
  public RETURN visitJoin(JoinPrel prel, EXTRA value) throws EXCEP;
  public RETURN visitProject(ProjectPrel prel, EXTRA value) throws EXCEP;

  public RETURN visitPrel(Prel prel, EXTRA value) throws EXCEP;

}
