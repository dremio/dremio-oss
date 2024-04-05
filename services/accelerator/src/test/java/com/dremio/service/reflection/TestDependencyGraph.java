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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.DirectProvider;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
import com.dremio.service.reflection.proto.DependencyType;
import com.dremio.service.reflection.proto.ReflectionDependencies;
import com.dremio.service.reflection.proto.ReflectionDependencyEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.test.DremioTest;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/** tests for {@link DependencyGraph} */
public class TestDependencyGraph {

  private static final LegacyKVStoreProvider kvstore =
      LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);

  private static DependenciesStore dependenciesStore;

  private static final ImmutableMap<String, DependencyEntry> dependencyByName =
      ImmutableMap.<String, DependencyEntry>builder()
          .put("pds1", DependencyEntry.of("pds1", Lists.newArrayList("f", "pds1"), 0L, null))
          .put("pds2", DependencyEntry.of("pds2", Lists.newArrayList("f", "pds2"), 0L, null))
          .put("raw1", DependencyEntry.of(rId("raw1"), 0L))
          .put("raw2", DependencyEntry.of(rId("raw2"), 0L))
          .put("agg1", DependencyEntry.of(rId("agg1"), 0L))
          .put("agg2", DependencyEntry.of(rId("agg2"), 0L))
          .put("agg3", DependencyEntry.of(rId("agg3"), 0L))
          .put("vds-raw", DependencyEntry.of(rId("vds-raw"), 0L))
          .put("vds-agg1", DependencyEntry.of(rId("vds-agg1"), 0L))
          .put("vds-agg2", DependencyEntry.of(rId("vds-agg2"), 0L))
          .put(
              "tablefunction1",
              DependencyEntry.of("tablefunction1", "postgres", "select * from emp"))
          .put(
              "tablefunction2",
              DependencyEntry.of("tablefunction2", "postgres", "select * from dept"))
          .build();

  @BeforeClass
  public static void beforeClass() throws Exception {
    kvstore.start();
    dependenciesStore = new DependenciesStore(DirectProvider.wrap(kvstore));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    kvstore.close();
  }

  @After
  public void afterTest() throws Exception {
    Iterable<Map.Entry<ReflectionId, ReflectionDependencies>> all = dependenciesStore.getAll();
    for (Map.Entry<ReflectionId, ReflectionDependencies> entry : all) {
      dependenciesStore.delete(entry.getKey());
    }
  }

  private static ReflectionId rId(String id) {
    return new ReflectionId(id);
  }

  private static Map<ReflectionId, ReflectionDependencies> storeDependencies(
      Multimap<String, String> dependencyMap) {
    ImmutableMap.Builder<ReflectionId, ReflectionDependencies> builder = ImmutableMap.builder();

    for (String id : dependencyMap.keySet()) {
      final ReflectionId reflectionId = rId(id);
      builder.put(
          reflectionId,
          new ReflectionDependencies()
              .setId(reflectionId)
              .setEntryList(
                  FluentIterable.from(dependencyMap.get(id))
                      .transform(
                          new Function<String, ReflectionDependencyEntry>() {
                            @Override
                            public ReflectionDependencyEntry apply(String input) {
                              return dependencyByName.get(input).toProtobuf();
                            }
                          })
                      .toList()));
    }

    return builder.build();
  }

  private static boolean isSuccessor(DependencyGraph graph, String parent, final String dependant) {
    return Iterables.any(
        graph.getSubGraph(rId(parent)),
        new Predicate<ReflectionId>() {
          @Override
          public boolean apply(ReflectionId input) {
            return input.getId().equals(dependant);
          }
        });
  }

  private static boolean isPredecessor(
      DependencyGraph graph, final String parent, final String dependant) {
    return Iterables.any(
        graph.getPredecessors(rId(dependant)),
        new Predicate<DependencyEntry>() {
          @Override
          public boolean apply(DependencyEntry entry) {
            return entry.getId().equals(parent);
          }
        });
  }

  @Test
  public void testLoadFromStore() throws Exception {
    final DependenciesStore dependenciesStore = Mockito.mock(DependenciesStore.class);

    final DependencyGraph graph = new DependencyGraph(dependenciesStore);

    // let's add some dependencies to the store
    // Map<Dependant, List<Parent>>
    final Multimap<String, String> dependencyMap =
        MultimapBuilder.hashKeys().arrayListValues().build();
    // pds1 > raw1 > agg1
    dependencyMap.put("raw1", "pds1");
    dependencyMap.put("raw1", "tablefunction1");
    dependencyMap.put("agg1", "raw1");
    // pds2 > raw2 > agg2
    dependencyMap.put("raw2", "pds2");
    dependencyMap.put("raw2", "tablefunction2");
    dependencyMap.put("agg2", "raw2");
    // raw2 > agg3
    dependencyMap.put("agg3", "raw2");
    // raw1, raw2 > vds-raw > vds-agg1
    dependencyMap.putAll("vds-raw", Lists.newArrayList("raw1", "raw2"));
    dependencyMap.put("vds-agg1", "vds-raw");
    // agg1, agg2 > vds-agg2
    dependencyMap.putAll("vds-agg2", Lists.newArrayList("agg1", "agg2"));

    Mockito.when(dependenciesStore.getAll())
        .thenReturn(storeDependencies(dependencyMap).entrySet());
    graph.loadFromStore();

    for (String dependant : dependencyMap.keySet()) {
      // we only store subgraphs of reflections
      for (String parent : dependencyMap.get(dependant)) {
        assertTrue(
            String.format("%s > %s", parent, dependant), isPredecessor(graph, parent, dependant));
        if (dependencyByName.get(parent).getType() == DependencyType.REFLECTION) {
          assertTrue(
              String.format("%s < %s", dependant, parent), isSuccessor(graph, parent, dependant));
        }
      }
    }
  }

  @Test
  public void testDeletePredecessor() throws Exception {
    final DependencyGraph graph = new DependencyGraph(dependenciesStore);

    // let's add some dependencies to the store. See dependencyMap above to imagine all the
    // dependencies
    ReflectionId pds1Id = new ReflectionId("pds1");
    ReflectionId pds2Id = new ReflectionId("pds2");
    ReflectionId raw1Id = new ReflectionId("raw1");
    ReflectionId agg1Id = new ReflectionId("agg1");
    ReflectionId raw2Id = new ReflectionId("raw2");
    ReflectionId agg2Id = new ReflectionId("agg2");
    ReflectionId agg3Id = new ReflectionId("agg3");
    ReflectionId vdsrawId = new ReflectionId("vds-raw");
    ReflectionId vdsagg1Id = new ReflectionId("vds-agg1");
    ReflectionId vdsagg2Id = new ReflectionId("vds-agg2");

    graph.setDependencies(
        raw1Id,
        Sets.<DependencyEntry>newHashSet(
            DependencyEntry.of("pds1", Lists.newArrayList("f", "pds1"), 0L, null)));
    graph.setDependencies(agg1Id, Sets.<DependencyEntry>newHashSet(DependencyEntry.of(raw1Id, 0L)));

    graph.setDependencies(
        raw2Id,
        Sets.<DependencyEntry>newHashSet(
            DependencyEntry.of("pds2", Lists.newArrayList("f", "pds2"), 0L, null)));
    graph.setDependencies(agg2Id, Sets.<DependencyEntry>newHashSet(DependencyEntry.of(raw2Id, 0L)));

    graph.setDependencies(agg3Id, Sets.<DependencyEntry>newHashSet(DependencyEntry.of(raw2Id, 0L)));

    graph.setDependencies(
        vdsrawId,
        Sets.<DependencyEntry>newHashSet(
            DependencyEntry.of(raw1Id, 0L), DependencyEntry.of(raw2Id, 0L)));

    graph.setDependencies(
        vdsagg1Id, Sets.<DependencyEntry>newHashSet(DependencyEntry.of(vdsrawId, 0L)));

    graph.setDependencies(
        vdsagg2Id,
        Sets.<DependencyEntry>newHashSet(
            DependencyEntry.of(agg1Id, 0L), DependencyEntry.of(agg2Id, 0L)));

    // loading all the data
    graph.loadFromStore();

    Iterable<ReflectionId> successorsChain = graph.getSubGraph(raw1Id);
    List<ReflectionId> scList = FluentIterable.from(successorsChain).toList();
    assertEquals(5, scList.size());
    assertTrue(scList.contains(vdsrawId));
    assertTrue(scList.contains(vdsagg1Id));
    assertTrue(scList.contains(vdsagg2Id));
    assertTrue(scList.contains(raw1Id));
    assertTrue(scList.contains(agg1Id));

    // check if data we are going to manipulate is loaded
    for (DependencyEntry depEntry : graph.getPredecessors(vdsagg1Id)) {
      assertEquals(((ReflectionDependency) depEntry).getReflectionId(), vdsrawId);
    }
    for (DependencyEntry depEntry : graph.getPredecessors(vdsrawId)) {
      assertTrue(
          ((ReflectionDependency) depEntry).getReflectionId().equals(raw2Id)
              || ((ReflectionDependency) depEntry).getReflectionId().equals(raw1Id));
    }

    assertTrue(isSuccessor(graph, raw1Id.getId(), vdsrawId.getId()));
    assertTrue(isSuccessor(graph, raw2Id.getId(), vdsrawId.getId()));

    // try to delete intermediate reflection
    graph.delete(vdsrawId);

    assertTrue(isSuccessor(graph, raw1Id.getId(), vdsagg1Id.getId()));

    Iterable<ReflectionId> successorsChain1 = graph.getSubGraph(raw1Id);
    List<ReflectionId> scList1 = FluentIterable.from(successorsChain1).toList();
    assertEquals(4, scList1.size());
    assertTrue(scList1.contains(vdsagg1Id));
    assertTrue(scList1.contains(vdsagg2Id));
    assertTrue(scList.contains(raw1Id));
    assertTrue(scList1.contains(agg1Id));

    List<DependencyEntry> preDvsagg1Id = graph.getPredecessors(vdsagg1Id);
    List<DependencyEntry> preDvdsagg2Id = graph.getPredecessors(vdsagg2Id);
    List<DependencyEntry> preDvdsrawId = graph.getPredecessors(vdsrawId);

    for (DependencyEntry depEntry : preDvsagg1Id) {
      assertNotEquals(((ReflectionDependency) depEntry).getReflectionId(), vdsrawId);
    }
    for (DependencyEntry depEntry : preDvdsagg2Id) {
      assertNotEquals(((ReflectionDependency) depEntry).getReflectionId(), vdsrawId);
    }
    assertTrue(preDvdsrawId.isEmpty());

    // simulate restart - create new Graph based on KVStore
    final DependencyGraph graph1 = new DependencyGraph(dependenciesStore);

    graph1.loadFromStore();

    assertTrue(isSuccessor(graph1, raw1Id.getId(), vdsagg1Id.getId()));

    Iterable<ReflectionId> successorsChain2 = graph.getSubGraph(raw1Id);
    List<ReflectionId> scList2 = FluentIterable.from(successorsChain2).toList();
    assertEquals(4, scList2.size());
    assertTrue(scList2.contains(vdsagg1Id));
    assertTrue(scList2.contains(vdsagg2Id));
    assertTrue(scList2.contains(raw1Id));
    assertTrue(scList2.contains(agg1Id));

    List<DependencyEntry> preDvsagg1Id1 = graph1.getPredecessors(vdsagg1Id);
    List<DependencyEntry> preDvdsagg2Id1 = graph1.getPredecessors(vdsagg2Id);
    List<DependencyEntry> preDvdsrawId1 = graph1.getPredecessors(vdsrawId);

    assertEquals(preDvdsagg2Id, preDvdsagg2Id1);
    assertEquals(preDvdsrawId, preDvdsrawId1);
    assertEquals(preDvsagg1Id, preDvsagg1Id1);

    for (DependencyEntry depEntry : preDvsagg1Id1) {
      assertNotEquals(((ReflectionDependency) depEntry).getReflectionId(), vdsrawId);
    }
    for (DependencyEntry depEntry : preDvdsagg2Id1) {
      assertNotEquals(((ReflectionDependency) depEntry).getReflectionId(), vdsrawId);
    }
    assertTrue(preDvdsrawId1.isEmpty());

    // try to delete intermediate again
    graph1.delete(agg1Id);

    Iterable<ReflectionId> successorsChain3 = graph1.getSubGraph(raw1Id);
    List<ReflectionId> scList3 = FluentIterable.from(successorsChain3).toList();
    assertEquals(3, scList3.size());
    assertTrue(scList3.contains(vdsagg1Id));
    assertTrue(scList3.contains(vdsagg2Id));
    assertTrue(scList3.contains(raw1Id));

    List<DependencyEntry> preDvsagg1Id2 = graph1.getPredecessors(vdsagg1Id);
    List<DependencyEntry> preDvdsagg2Id2 = graph1.getPredecessors(vdsagg2Id);

    // check if in-memory data is correct
    assertEquals(preDvsagg1Id1, preDvsagg1Id2);
    assertNotEquals(preDvdsagg2Id1, preDvdsagg2Id2);

    assertFalse(preDvdsagg2Id2.contains(DependencyEntry.of(agg1Id, 0L)));
    assertTrue(preDvdsagg2Id1.contains(DependencyEntry.of(agg1Id, 0L)));

    // simulate another restart
    final DependencyGraph graph2 = new DependencyGraph(dependenciesStore);

    graph2.loadFromStore();

    Iterable<ReflectionId> successorsChain4 = graph2.getSubGraph(raw1Id);
    List<ReflectionId> scList4 = FluentIterable.from(successorsChain4).toList();
    assertEquals(3, scList4.size());
    assertTrue(scList4.contains(vdsagg1Id));
    assertTrue(scList4.contains(vdsagg2Id));
    assertTrue(scList4.contains(raw1Id));

    List<DependencyEntry> preDvsagg1Id3 = graph2.getPredecessors(vdsagg1Id);
    List<DependencyEntry> preDvdsagg2Id3 = graph2.getPredecessors(vdsagg2Id);

    // after restart is correct
    assertEquals(preDvsagg1Id1, preDvsagg1Id3);
    assertNotEquals(preDvdsagg2Id1, preDvdsagg2Id3);
    assertFalse(preDvdsagg2Id3.contains(DependencyEntry.of(agg1Id, 0L)));

    // in-memory and reloaded are the same
    assertEquals(preDvdsagg2Id2, preDvdsagg2Id3);
  }

  @Test
  public void testSnapshotDependency() throws Exception {
    final DependencyGraph graph = new DependencyGraph(dependenciesStore);

    // let's add some dependencies to the store. See dependencyMap above to imagine all the
    // dependencies
    ReflectionId raw1Id = new ReflectionId("raw1");
    ReflectionId raw2Id = new ReflectionId("raw2");
    ReflectionId vdsrawId = new ReflectionId("vds-raw");
    ReflectionId vdsagg1Id = new ReflectionId("vds-agg1");

    // now create some snapshot IDs to go with each of the above
    long pds1SnapshotId = new Random().nextLong();
    long raw1SnapshotId = new Random().nextLong();
    long raw2SnapshotId = new Random().nextLong();
    long vdsrawSnapshotId = new Random().nextLong();

    graph.setDependencies(
        raw1Id,
        Sets.newHashSet(
            DependencyEntry.of("pds1", Lists.newArrayList("f", "pds1"), pds1SnapshotId, null)));
    graph.setDependencies(
        vdsrawId,
        Sets.newHashSet(
            DependencyEntry.of(raw1Id, raw1SnapshotId),
            DependencyEntry.of(raw2Id, raw2SnapshotId)));
    graph.setDependencies(
        vdsagg1Id, Sets.newHashSet(DependencyEntry.of(vdsrawId, vdsrawSnapshotId)));

    // loading all the data
    graph.loadFromStore();

    // check that raw1 depends on pds1 snapshot pds1SnapshotId
    List<DependencyEntry> raw1Deps = graph.getPredecessors(raw1Id);
    assertEquals(1, raw1Deps.size());

    DependencyEntry raw1Dep = raw1Deps.get(0);
    assertTrue(raw1Dep.getType().equals(DependencyType.DATASET));
    assertEquals(pds1SnapshotId, ((DependencyEntry.DatasetDependency) raw1Dep).getSnapshotId());

    // check that vdsrraw depends on raw1 and raw2 and their snapshots
    for (DependencyEntry depEntry : graph.getPredecessors(vdsrawId)) {
      assertTrue(
          ((ReflectionDependency) depEntry).getReflectionId().equals(raw2Id)
              || ((ReflectionDependency) depEntry).getReflectionId().equals(raw1Id));
      assertTrue(
          (((ReflectionDependency) depEntry).getSnapshotId() == raw2SnapshotId)
              || ((ReflectionDependency) depEntry).getSnapshotId() == raw1SnapshotId);
    }

    // now check that vds-agg1 depends on vdsraw
    List<DependencyEntry> vdsAgg1Deps = graph.getPredecessors(vdsagg1Id);
    assertEquals(1, vdsAgg1Deps.size());

    DependencyEntry vdsAgg1Dep = vdsAgg1Deps.get(0);
    assertTrue(vdsAgg1Dep.getType().equals(DependencyType.REFLECTION));
    assertEquals(vdsrawSnapshotId, ((ReflectionDependency) vdsAgg1Dep).getSnapshotId());

    // now simulate a refresh of raw1 by updating the snapshot of the dependency
    long pds1NewSnapshotId = new Random().nextLong();
    graph.setDependencies(
        raw1Id,
        Sets.newHashSet(
            DependencyEntry.of("pds1", Lists.newArrayList("f", "pds1"), pds1NewSnapshotId, null)));

    // ensure it was actually updated
    raw1Deps = graph.getPredecessors(raw1Id);
    assertEquals(1, raw1Deps.size());

    raw1Dep = raw1Deps.get(0);
    assertTrue(raw1Dep.getType().equals(DependencyType.DATASET));
    assertEquals(pds1NewSnapshotId, ((DependencyEntry.DatasetDependency) raw1Dep).getSnapshotId());
  }
}
