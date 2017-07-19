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
package com.dremio.exec.store.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * The AssignmentCreator is responsible for assigning a set of work units to the available slices.
 */
public class AssignmentCreator<T extends CompleteWork> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AssignmentCreator.class);

  /**
   * Comparator used to sort in order of decreasing affinity
   */
  private static Comparator<Entry<NodeEndpoint,Long>> comparator = new Comparator<Entry<NodeEndpoint,Long>>() {
    @Override
    public int compare(Entry<NodeEndpoint, Long> o1, Entry<NodeEndpoint,Long> o2) {
      return o1.getValue().compareTo(o2.getValue());
    }
  };

  private static Comparator<EndpointAffinity> COMPARATOR = new Comparator<EndpointAffinity>() {
    @Override
    public int compare(EndpointAffinity o1, EndpointAffinity o2) {
      return Double.compare(o1.getAffinity(), o2.getAffinity());
    }
  };

  /**
   * the maximum number of work units to assign to any minor fragment
   */
  private int maxWork;

  /**
   * The units of work to be assigned
   */
  private List<T> units;

  /**
   * Mappings
   */
  private ArrayListMultimap<Integer, T> mappings = ArrayListMultimap.create();

  /**
   * A list of NodeEndpoints, where the index in the list corresponds to the minor fragment id
   */
  private List<NodeEndpoint> incomingEndpoints;

  private AssignmentCreator(List<NodeEndpoint> incomingEndpoints, List<T> units) {
    this.incomingEndpoints = incomingEndpoints;
    this.units = units;
  }


  /**
   * Assign each unit of work to a minor fragment, given that a list of NodeEndpoints, whose index in the list correspond
   * to the minor fragment id for each fragment. A given NodeEndpoint can appear multiple times in this list. This method
   * will try to assign work based on the affinity of each work unit, but will also evenly distribute the work units among
   * all of the minor fragments
   *
   * @param incomingEndpoints The list of incomingEndpoints, indexed by minor fragment id
   * @param units the list of work units to be assigned
   * @return A multimap that maps each minor fragment id to a list of work units
   */
  public static <T extends CompleteWork> ListMultimap<Integer,T> getMappings(List<NodeEndpoint> incomingEndpoints, List<T> units) {
    AssignmentCreator<T> creator = new AssignmentCreator<>(incomingEndpoints, units);
    return creator.getMappings();
  }

  /**
   * Does the work of creating the mappings for this AssignmentCreator
   * @return the minor fragment id to work units mapping
   */
  private ListMultimap<Integer, T> getMappings() {
    Stopwatch watch = Stopwatch.createStarted();
    maxWork = (int) Math.ceil(units.size() / ((float) incomingEndpoints.size()));
    LinkedList<WorkEndpointListPair<T>> workList = getWorkList();
    LinkedList<WorkEndpointListPair<T>> unassignedWorkList;
    Map<NodeEndpoint,FragIteratorWrapper> endpointIterators = getEndpointIterators();

    unassignedWorkList = assign(workList, endpointIterators, true);

    assignLeftovers(unassignedWorkList, endpointIterators, true);
    assignLeftovers(unassignedWorkList, endpointIterators, false);

    if (!unassignedWorkList.isEmpty()) {
      throw new IllegalStateException("There are still unassigned work units");
    }

    logger.debug("Took {} ms to assign {} work units to {} fragments", watch.elapsed(TimeUnit.MILLISECONDS), units.size(), incomingEndpoints.size());
    return mappings;
  }

  /**
   *
   * @param workList the list of work units to assign
   * @param endpointIterators the endpointIterators to assign to
   * @param assignMinimum whether to assign only up to the minimum required
   * @return a list of unassigned work units
   */
  private LinkedList<WorkEndpointListPair<T>> assign(List<WorkEndpointListPair<T>> workList, Map<NodeEndpoint,FragIteratorWrapper> endpointIterators, boolean assignMinimum) {
    LinkedList<WorkEndpointListPair<T>> currentUnassignedList = Lists.newLinkedList();
    outer: for (WorkEndpointListPair<T> workPair : workList) {
      List<NodeEndpoint> endpoints = workPair.sortedEndpoints;
      for (NodeEndpoint endpoint : endpoints) {
        FragIteratorWrapper iteratorWrapper = endpointIterators.get(endpoint);
        if (iteratorWrapper == null) {
          continue;
        }
        if (iteratorWrapper.count < (assignMinimum ? iteratorWrapper.minCount : iteratorWrapper.maxCount)) {
          Integer assignment = iteratorWrapper.iter.next();
          iteratorWrapper.count++;
          mappings.put(assignment, workPair.work);
          continue outer;
        }
      }
      currentUnassignedList.add(workPair);
    }
    return currentUnassignedList;
  }

  /**
   *
   * @param unassignedWorkList the work units to assign
   * @param endpointIterators the endpointIterators to assign to
   * @param assignMinimum wheterh to assign the minimum amount
   */
  private void assignLeftovers(LinkedList<WorkEndpointListPair<T>> unassignedWorkList, Map<NodeEndpoint,FragIteratorWrapper> endpointIterators, boolean assignMinimum) {
    outer: for (FragIteratorWrapper iteratorWrapper : endpointIterators.values()) {
      while (iteratorWrapper.count < (assignMinimum ? iteratorWrapper.minCount : iteratorWrapper.maxCount)) {
        WorkEndpointListPair<T> workPair = unassignedWorkList.poll();
        if (workPair == null) {
          break outer;
        }
        Integer assignment = iteratorWrapper.iter.next();
        iteratorWrapper.count++;
        mappings.put(assignment, workPair.work);
      }
    }
  }

  /**
   * Builds the list of WorkEndpointListPairs, which pair a work unit with a list of endpoints sorted by affinity
   * @return the list of WorkEndpointListPairs
   */
  private LinkedList<WorkEndpointListPair<T>> getWorkList() {

    LinkedList<WorkEndpointListPair<T>> workList = Lists.newLinkedList();
    for (T work : units) {
      List<EndpointAffinity> entries = new ArrayList<>(work.getAffinity());
      Collections.sort(entries, COMPARATOR);

      List<NodeEndpoint> sortedEndpoints = Lists.newArrayList();
      for (EndpointAffinity ea : entries) {
        sortedEndpoints.add(ea.getEndpoint());
      }
      workList.add(new WorkEndpointListPair<>(work, sortedEndpoints));
    }
    return workList;
  }

  /**
   *  A wrapper class around a work unit and its associated sort list of Endpoints (sorted by affinity in decreasing order)
   */
  private static class WorkEndpointListPair<T> {
    T work;
    List<NodeEndpoint> sortedEndpoints;

    WorkEndpointListPair(T work, List<NodeEndpoint> sortedEndpoints) {
      this.work = work;
      this.sortedEndpoints = sortedEndpoints;
    }
  }

  /**
   * Groups minor fragments together by corresponding endpoint, and creates an iterator that can be used to evenly
   * distribute work assigned to a given endpoint to all corresponding minor fragments evenly
   *
   * @return
   */
  private Map<NodeEndpoint,FragIteratorWrapper> getEndpointIterators() {
    Stopwatch watch = Stopwatch.createStarted();
    Map<NodeEndpoint,FragIteratorWrapper> map = Maps.newLinkedHashMap();
    Map<NodeEndpoint,List<Integer>> mmap = Maps.newLinkedHashMap();
    for (int i = 0; i < incomingEndpoints.size(); i++) {
      NodeEndpoint endpoint = incomingEndpoints.get(i);
      List<Integer> intList = mmap.get(incomingEndpoints.get(i));
      if (intList == null) {
        intList = Lists.newArrayList();
      }
      intList.add(Integer.valueOf(i));
      mmap.put(endpoint, intList);
    }

    for (NodeEndpoint endpoint : mmap.keySet()) {
      FragIteratorWrapper wrapper = new FragIteratorWrapper();
      wrapper.iter = Iterators.cycle(mmap.get(endpoint));
      wrapper.maxCount = maxWork * mmap.get(endpoint).size();
      wrapper.minCount = Math.max(maxWork - 1, 1) * mmap.get(endpoint).size();
      map.put(endpoint, wrapper);
    }
    return map;
  }

  /**
   * A struct that holds a fragment iterator and keeps track of how many units have been assigned, as well as the maximum
   * number of assignment it will accept
   */
  private static class FragIteratorWrapper {
    int count = 0;
    int maxCount;
    int minCount;
    Iterator<Integer> iter;
  }

}
