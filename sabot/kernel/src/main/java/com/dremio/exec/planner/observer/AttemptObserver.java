/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.observer;

import java.util.List;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.sabot.op.screen.QueryWritableBatch;

public interface AttemptObserver {

  /**
   * Query was started.
   * @param query Query configuration
   * @param user User.
   */
  void queryStarted(UserRequest query, String user);

  /**
   * Planning started using provided plan.
   * @param rawPlan Typically SQL but could also be a logical or physical plan.
   */
  void planStart(String rawPlan);

  /**
   * Parsing of query completed and validated.
   * @param rowType The validated row type.
   * @param node The AST of the validated SQL
   * @param millisTaken
   */
  void planValidated(RelDataType rowType, SqlNode node, long millisTaken);

  /**
   * Plan that is serializable, just before convertible scans are converted
   * @param serializable
   */
  void planSerializable(RelNode serializable);

  /**
   * Convert validated query to rel tree
   * @param converted rel tree generated from validated query
   * @param millisTaken
   */
  void planConvertedToRel(RelNode converted, long millisTaken);


  /**
   * Convert Scan query
   * @param converted rel tree generated from query
   * @param millisTaken
   */
  void planConvertedScan(RelNode converted, long millisTaken);

  /**
   * A view just expanded into a rel tree.
   * @param expanded The new rel tree that will be used in place of the defined view.
   * @param schemaPath The schema path of the view.
   * @param nestingLevel The amount of nesting of the view.
   * @param sql The sql associated with the view.
   */
  void planExpandView(RelRoot expanded, List<String> schemaPath, int nestingLevel, String sql);

  /**
   * Called multiple times, describing transformations that occurred during planning.
   * @param phase The phase of planning that was run.
   * @param planner The planner used to do this transformation.
   * @param before The graph before the transformation occurred.
   * @param after The graph after the planning transformation took place
   * @param millisTaken The amount of time taken to complete the planning.
   */
  void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after, long millisTaken);

  /**
   * The text of the final query plan was produced.
   * @param text Text based explain plan.
   * @param millisTaken
   */
  void planText(String text, long millisTaken);


  /**
   * Parallelization planning started
   */
  void planParallelStart();

  /**
   * The decisions made for parallelizations and fragments were completed.
   * @param planningSet
   */
  void planParallelized(PlanningSet planningSet);

  /**
   * The decisions for distribution of work are completed.
   * @param unit The distribution decided for each node.
   */
  void plansDistributionComplete(QueryWorkUnit unit);

  /**
   * Report applicable materializations
   */
  void planFindMaterializations(long millisTaken);

  /**
   * Report normalization completion
   */
  void planNormalized(long millisTaken, List<RelNode> normalizedQueryPlans);

  /**
   * Report substitution
   * @param materialization
   * @param substitutions number of plans returned after substitution finished
   * @param query
   * @param target
   */
  void planSubstituted(DremioRelOptMaterialization materialization,
                       List<RelNode> substitutions,
                       RelNode target, long millisTaken);

  /**
   * Report materializations used to accelerate incoming query only if query is accelerated.
   *
   * @param info acceleration info.
   */
  void planAccelerated(SubstitutionInfo info);

  /**
   * The planning and parallelization phase of the query is completed.
   *
   * An {@link ExecutionPlan execution plan} is provided to observer.
   */
  void planCompleted(ExecutionPlan plan);

  /**
   * The execution of the query started.
   * @param profile The initial query profile for the query.
   */
  void execStarted(QueryProfile profile);

  /**
   * Some data is now returned from the query.
   * @param outcomeListener Listener used to inform that observer is done consuming data.
   * @param result The data to consume.
   */
  void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result);

  @Deprecated
  /**
   * Exists due to existing stuff but needs to be removed.
   * @param text
   */
  void planJsonPlan(String text);


  /**
   * The current query attempt is completed and has been cleaned up.
   * Another attempt may be started after this one, but will use a new instance of AttemptObserver
   *
   * @param result The result of the query.
   */
  void attemptCompletion(UserResult result);


  /**
   * Time taken to generate fragments.
   * @param millisTaken time in milliseconds
   */
  void planGenerationTime(long millisTaken);

  /**
   * Time taken to assign fragments to nodes.
   * @param millisTaken time in milliseconds
   */
  void planAssignmentTime(long millisTaken);

  /**
   * Time taken for sending intermediate fragments to all nodes.
   * @param millisTaken
   */
  void intermediateFragmentScheduling(long millisTaken);

  /**
   * Time taken for sending leaf fragments to all nodes.
   * @param millisTaken
   */
  void leafFragmentScheduling(long millisTaken);

}
