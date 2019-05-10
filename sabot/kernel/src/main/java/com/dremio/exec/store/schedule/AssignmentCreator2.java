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
package com.dremio.exec.store.schedule;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.SplitWork;
import com.dremio.service.Pointer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * The AssignmentCreator is responsible for assigning a set of work units to the available slices.
 */
public class AssignmentCreator2<T extends CompleteWork> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AssignmentCreator2.class);

  private final List<WorkWrapper> workList;
  private final Map<String,HostFragments> hostFragmentMap;
  private final long maxSize;

  public static <T extends CompleteWork> ListMultimap<Integer, T>
  getMappings(List<NodeEndpoint> incomingEndpoints, List<T> units, double balanceFactor) {
    checkArgument(incomingEndpoints.size() > 0, "No executors available to assign work.");
    AssignmentCreator2<T> creator = new AssignmentCreator2<>(incomingEndpoints, units, balanceFactor);
    return creator.makeAssignments();
  }

  AssignmentCreator2(List<NodeEndpoint> incomingEndpoints, List<T> units, double balanceFactor) {
    this.workList = createWorkList(units);
    int unitsPerFragment = (int) Math.ceil(units.size() / (float) incomingEndpoints.size());
    this.maxSize = (long) (sumOfFirst(units, unitsPerFragment) * balanceFactor);
    this.hostFragmentMap = createHostFragmentsMap(incomingEndpoints);
  }

  private long sumOfFirst(List<T> units, int count) {
    long sum = 0;
    for (int i = 0; i < count && i < units.size(); i++) {
      sum += units.get(i).getTotalBytes();
    }
    return sum;
  }

  ListMultimap<Integer,T> makeAssignments() {
    List<WorkWrapper> unassigned = new ArrayList<>();

    for (WorkWrapper work : workList) {
      boolean assigned = assignWork(work);
      if (!assigned) {
        unassigned.add(work);
      }
    }

    int unassignedCount = unassigned.size();
    assignLeftOvers(unassigned);
    logger.debug("Items assigned. With affinity: {}, Random: {}", workList.size() - unassignedCount, unassignedCount);

    ListMultimap<Integer,T> result = ArrayListMultimap.create();

    if(logger.isDebugEnabled()) {
      for (FragmentWork fragment : getFragments()) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("FragId: %d, Cost: %d\n", fragment.fragmentId, fragment.totalSize));
        for(WorkWrapper w : fragment.workList) {
          sb.append("\t");
          sb.append(toString(w, true));
          sb.append("\n");
        }
        logger.debug(sb.toString());
      }
    }

    final Pointer<Integer> workCount = new Pointer<>(0);
    for (FragmentWork fragment : getFragments()) {
      result.putAll(fragment.fragmentId, fragment.workList.stream()
          .map(workWrapper -> {
            workCount.value++;
            return workWrapper.work;
          })
          .collect(Collectors.toList()));
    }

    Preconditions.checkState(workCount.value == workList.size());
    return result;
  }

  /**
   * assign the remaining work units to hosts/fragments based on least load
   * @param leftOvers
   */
  private void assignLeftOvers(List<WorkWrapper> leftOvers) {
    PriorityQueue<FragmentWork> queue = new PriorityQueue<>();
    List<FragmentWork> fragments = getFragments();
    queue.addAll(fragments);

    for (WorkWrapper work : leftOvers) {
      FragmentWork fragment = queue.poll();
      fragment.addWork(work);
      queue.add(fragment);
    }
  }

  private List<FragmentWork> getFragments() {
    return hostFragmentMap.values()
        .stream()
        .flatMap(t -> t.fragmentQueue.stream())
        .collect(Collectors.toList());
  }

  /**
   * Attempt to assign the unit of work to a host which has affinity, choosing the host which has the currently least
   * loaded fragment. If there are no hosts in the affinity list, or if adding the work would put the load above
   * the limit, it will not assign the work
   * @param work
   * @return true if able to assign the work
   */
  private boolean assignWork(WorkWrapper work) {
    List<HostFragments> hostFragmentsList = new ArrayList<>();

    if(work.hosts.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Failed to assign because work has no affinity. Work: {}", toString(work, false));
      }
      return false;
    }

    for (String host : work.hosts) {
      HostFragments hostFragments = hostFragmentMap.get(host);
      if (hostFragments != null) {
        hostFragmentsList.add(hostFragments);
      }
    }
    if (hostFragmentsList.size() == 0) {
      if(logger.isDebugEnabled()) {
        logger.debug("Failed to assign because the we weren't able to find any scheduleable host that matched those recorded. Work: {}, ", toString(work, false));
      }
      return false;
    }
    Collections.sort(hostFragmentsList);
    HostFragments hostFragments = hostFragmentsList.get(0);
    long peekSize = hostFragments.peekSize();
    long workSize = work.work.getTotalBytes();
    if (peekSize + workSize > maxSize) {
      logger.debug("Failed to assign because Fragments size + this work size is greater than max size: {} + {} > {}. Work: {}", peekSize, workSize, maxSize, toString(work, false));
      return false;
    }
    hostFragments.addWork(work);
    return true;
  }

  private String toString(WorkWrapper w, boolean includeHosts) {
    StringBuilder sb = new StringBuilder();
    if (includeHosts) {
      sb.append("Hosts: ,");
      sb.append(w.hosts);
      sb.append(", ");
    }
    sb.append("Bytes: ");
    sb.append(w.work.getTotalBytes());
    sb.append(", Node affinity: ");
    for(EndpointAffinity ea : w.work.getAffinity()) {
      sb.append(ea.getEndpoint().getAddress() + ":" + ea.getEndpoint().getFabricPort() + "=" + ea.getAffinity());
      sb.append(",");
    }
//    sb.append(w.work.getAffinity());
    if(w.work instanceof SplitWork) {
      SplitWork sw = (SplitWork) w.work;
      sb.append(", Split key: ");
      sb.append(sw.getSplitInfo().getSplitKey());
    }
    return sb.toString();
  }

  private Map<String,HostFragments> createHostFragmentsMap(List<NodeEndpoint> incomingEndpoints) {
    Multimap<String,Integer> endpointMap = ArrayListMultimap.create();
    for (int i = 0; i < incomingEndpoints.size(); i++) {
      String host = incomingEndpoints.get(i).getAddress();
      endpointMap.put(host, i);
    }

    List<HostFragments> hostFragments = new ArrayList<>();
    for (Entry<String,Collection<Integer>> entry : endpointMap.asMap().entrySet()) {
      hostFragments.add(new HostFragments(entry.getKey(), entry.getValue()));
    }
    return FluentIterable.from(hostFragments)
      .uniqueIndex(new Function<HostFragments, String>() {
        @Override
        public String apply(HostFragments hostFragment) {
          return hostFragment.host;
        }
      });
  }

  private List<WorkWrapper> createWorkList(List<T> completeWorkList) {
    List<WorkWrapper> workList = new ArrayList<>();
    for (T work : completeWorkList) {
      workList.add(new WorkWrapper(work));
    }

    Collections.sort(workList);

    // we want largest work units first in the list
    return Lists.reverse(workList);
  }

  /**
   * Class which encapsulates a host and the fragments which are running on the host
   */
  private class HostFragments implements Comparable<HostFragments> {
    private final String host;
    private final PriorityQueue<FragmentWork> fragmentQueue = new PriorityQueue<>();

    private HostFragments(String host, Collection<Integer> fragments) {
      this.host = host;
      for (Integer id : fragments) {
        fragmentQueue.add(new FragmentWork(id));
      }
    }

    private long peekSize() {
      return fragmentQueue.peek().totalSize;
    }

    /**
     * Add a unit of work to the fragment which currently holds the least work on this host
     * @param work
     */
    private void addWork(WorkWrapper work) {
      FragmentWork fragmentWork = fragmentQueue.poll();
      fragmentWork.addWork(work);
      fragmentQueue.add(fragmentWork);
    }

    @Override
    public int compareTo(HostFragments o) {
      return Long.compare(peekSize(), o.peekSize());
    }
  }

  /**
   * A class which holds the work units for a particular fragment
   */
  private class FragmentWork implements Comparable<FragmentWork> {
    private final int fragmentId;
    private List<WorkWrapper> workList = new ArrayList<>();
    private long totalSize = 0;

    private FragmentWork(int fragmentId) {
      this.fragmentId = fragmentId;
    }

    private void addWork(WorkWrapper work) {
      workList.add(work);
      totalSize += work.work.getTotalBytes();
    }

    @Override
    public int compareTo(FragmentWork o) {
      return Long.compare(totalSize, o.totalSize);
    }
  }

  /**
   * A wrapper around CompleteWork, which simplifies the EndpointAffinity, and instead only lists the hosts which
   * have more than 1/2 of the total bytes local
   */
  private class WorkWrapper implements Comparable<WorkWrapper> {
    private final T work;
    private final Set<String> hosts;

    private WorkWrapper(T work) {
      this.work = work;
      ImmutableSet.Builder<String> hostsBuilder = ImmutableSet.builder();
      for (EndpointAffinity ea : work.getAffinity()) {

        if (ea.getAffinity() >= work.getTotalBytes() / 2) {
          hostsBuilder.add(ea.getEndpoint().getAddress());
        }
      }
      this.hosts = hostsBuilder.build();
    }

    @Override
    public int compareTo(WorkWrapper o) {
      return work.compareTo(o.work);
    }
  }

}
