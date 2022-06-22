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
package com.dremio.plugins.elastic;

import java.util.Collections;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.elastic.planning.rels.ElasticScanPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchPrel;
import com.dremio.plugins.elastic.planning.rels.ScanBuilder;

/**
 * Test ElasticScanPrel.
 */
public class TestElasticScanPrel {
  private static final RelTraitSet EMPTY_TRAIT = RelTraitSet.createEmpty();
  private ElasticScanPrel prel;

  @Before
  public void setup() {
    final RelOptCluster cluster = Mockito.mock(RelOptCluster.class);
    final ElasticsearchPrel scanPrel = Mockito.mock(ElasticsearchPrel.class);
    final ScanBuilder builder = Mockito.mock(ScanBuilder.class);
    Mockito.when(builder.getTable()).thenReturn(Mockito.mock(RelOptTable.class));
    prel = new ElasticScanPrel(
      cluster,
      EMPTY_TRAIT,
      scanPrel,
      builder);
  }

  @Test
  public void testCopyEqual() {
    final RelNode copyPrel = prel.copy(EMPTY_TRAIT, Collections.emptyList());

    // This should be the same object.
    Assert.assertSame(prel, copyPrel);
  }

  @Test
  public void testCopy() {
    final RelTraitSet newTraits = RelTraitSet.createEmpty().plus(Prel.PHYSICAL);
    final RelNode copyPrel = prel.copy(newTraits, Collections.emptyList());
    Assert.assertTrue(copyPrel instanceof ElasticScanPrel);
    final ElasticScanPrel copyESPrel = (ElasticScanPrel)copyPrel;

    // This should not be the same object, since the traits are different.
    Assert.assertNotSame(prel, copyPrel);
    Assert.assertEquals(prel.getOriginPrel(), copyESPrel.getOriginPrel());
    Assert.assertEquals(prel.getMinParallelizationWidth(), copyESPrel.getMinParallelizationWidth());
    Assert.assertEquals(prel.getMaxParallelizationWidth(), copyESPrel.getMaxParallelizationWidth());
    Assert.assertEquals(newTraits, copyESPrel.getTraitSet());
  }
}
