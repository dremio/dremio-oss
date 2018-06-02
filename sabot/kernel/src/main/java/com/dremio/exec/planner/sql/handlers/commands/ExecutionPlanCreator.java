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
package com.dremio.exec.planner.sql.handlers.commands;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.MakeFragmentsVisitor;
import com.dremio.exec.planner.fragment.SimpleParallelizer;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.handlers.commands.AsyncCommand.QueueType;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.server.options.OptionList;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.util.MemoryAllocationUtilities;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.fasterxml.jackson.databind.ObjectMapper;

class ExecutionPlanCreator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecutionPlanCreator.class);

  public static ExecutionPlan getExecutionPlan(
      final QueryContext queryContext,
      final PhysicalPlanReader reader,
      AttemptObserver observer,
      final PhysicalPlan plan,
      final QueueType queueType) throws ExecutionSetupException {

    // step one, check to make sure that there available execution nodes.
    Collection<NodeEndpoint> endpoints = queryContext.getActiveEndpoints();
    if(endpoints.isEmpty()){
      throw UserException.resourceError().message("No executors currently available.").build(logger);
    }

    // control memory settings for sorts.
     MemoryAllocationUtilities.setupSortMemoryAllocations(plan, queryContext.getOptions(),
         queryContext.getClusterResourceInformation());

    final Root rootOperator = plan.getRoot();
    final Fragment rootOperatorFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);
    final SimpleParallelizer parallelizer = new SimpleParallelizer(queryContext, observer);
    // pass all query, session and non-default system options to the fragments
    final OptionList fragmentOptions = queryContext.getNonDefaultOptions();

    CoordExecRPC.QueryContextInformation queryContextInformation = queryContext.getQueryContextInfo();

    // update query limit based on the queueType
    final OptionManager options = queryContext.getOptions();
    final boolean memoryControlEnabled = options.getOption(ExecConstants.ENABLE_QUEUE_MEMORY_LIMIT);
    final long memoryLimit = (queueType == QueueType.SMALL) ?
      options.getOption(ExecConstants.SMALL_QUEUE_MEMORY_LIMIT):
      options.getOption(ExecConstants.LARGE_QUEUE_MEMORY_LIMIT);
    if (memoryControlEnabled && memoryLimit > 0) {
      final long queryMaxAllocation = queryContext.getQueryContextInfo().getQueryMaxAllocation();
      queryContextInformation = CoordExecRPC.QueryContextInformation.newBuilder(queryContextInformation)
        .setQueryMaxAllocation(Math.min(memoryLimit, queryMaxAllocation)).build();
    }

    final List<PlanFragment> planFragments = parallelizer.getFragments(
        fragmentOptions,
        queryContext.getCurrentEndpoint(),
        queryContext.getQueryId(),
        endpoints,
        reader,
        rootOperatorFragment,
        queryContext.getSession(),
        queryContextInformation,
        queryContext.getFunctionRegistry());

    if (logger.isTraceEnabled()) {
      final StringBuilder sb = new StringBuilder();
      sb.append("PlanFragments for query ");
      sb.append(queryContext.getQueryId());
      sb.append('\n');

      final int fragmentCount = planFragments.size();
      int fragmentIndex = 0;
      for(final PlanFragment planFragment : planFragments) {
        final FragmentHandle fragmentHandle = planFragment.getHandle();
        sb.append("PlanFragment(");
        sb.append(++fragmentIndex);
        sb.append('/');
        sb.append(fragmentCount);
        sb.append(") major_fragment_id ");
        sb.append(fragmentHandle.getMajorFragmentId());
        sb.append(" minor_fragment_id ");
        sb.append(fragmentHandle.getMinorFragmentId());
        sb.append('\n');

        final NodeEndpoint endpointAssignment = planFragment.getAssignment();
        sb.append("  NodeEndpoint address ");
        sb.append(endpointAssignment.getAddress());
        sb.append('\n');

        String jsonString = "<<malformed JSON>>";
        sb.append("  fragment_json: ");
        final ObjectMapper objectMapper = new ObjectMapper();
        try(InputStream is = PhysicalPlanReader.toInputStream(planFragment.getFragmentJson(), planFragment.getFragmentCodec()))
        {

          final Object json = objectMapper.readValue(is, Object.class);
          jsonString = objectMapper.writeValueAsString(json);
        } catch(final Exception e) {
          // we've already set jsonString to a fallback value
        }
        sb.append(jsonString);

        logger.trace(sb.toString());
      }
    }

    return new ExecutionPlan(plan, planFragments);
  }
}
