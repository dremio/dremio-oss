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
package com.dremio.sabot.op.common.hashtable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.ValueVectorWriteExpression;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.common.hashtable.HashTable.BatchAddedListener;
import com.dremio.sabot.op.join.JoinUtils;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;


public class ChainedHashTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ChainedHashTable.class);

  private static final GeneratorMapping KEY_MATCH_BUILD =
      GeneratorMapping.create("setupInterior" /* setup method */, "isKeyMatchInternalBuild" /* eval method */,
          null /* reset */, null /* cleanup */);

  private static final GeneratorMapping KEY_MATCH_PROBE =
      GeneratorMapping.create("setupInterior" /* setup method */, "isKeyMatchInternalProbe" /* eval method */,
          null /* reset */, null /* cleanup */);

  private static final GeneratorMapping GET_HASH_BUILD =
      GeneratorMapping.create("doSetup" /* setup method */, "getHashBuild" /* eval method */,
          null /* reset */, null /* cleanup */);

  private static final GeneratorMapping GET_HASH_PROBE =
      GeneratorMapping.create("doSetup" /* setup method */, "getHashProbe" /* eval method */, null /* reset */,
          null /* cleanup */);

  private static final GeneratorMapping SET_VALUE =
      GeneratorMapping.create("setupInterior" /* setup method */, "setValue" /* eval method */, null /* reset */,
          null /* cleanup */);

  private static final GeneratorMapping OUTPUT_KEYS =
      GeneratorMapping.create("setupInterior" /* setup method */, "outputRecordKeys" /* eval method */,
          null /* reset */, null /* cleanup */);

  // GM for putting constant expression into method "setupInterior"
  private static final GeneratorMapping SETUP_INTERIOR_CONSTANT =
      GeneratorMapping.create("setupInterior" /* setup method */, "setupInterior" /* eval method */,
          null /* reset */, null /* cleanup */);

  // GM for putting constant expression into method "doSetup"
  private static final GeneratorMapping DO_SETUP_CONSTANT =
      GeneratorMapping.create("doSetup" /* setup method */, "doSetup" /* eval method */, null /* reset */,
          null /* cleanup */);

  private final MappingSet KeyMatchIncomingBuildMapping =
      new MappingSet("incomingRowIdx", null, "incomingBuild", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_BUILD);
  private final MappingSet KeyMatchIncomingProbeMapping =
      new MappingSet("incomingRowIdx", null, "incomingProbe", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_PROBE);
  private final MappingSet KeyMatchHtableMapping =
      new MappingSet("htRowIdx", null, "htContainer", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_BUILD);
  private final MappingSet KeyMatchHtableProbeMapping =
      new MappingSet("htRowIdx", null, "htContainer", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_PROBE);
  private final MappingSet GetHashIncomingBuildMapping =
      new MappingSet("incomingRowIdx", null, "incomingBuild", null, DO_SETUP_CONSTANT, GET_HASH_BUILD);
  private final MappingSet GetHashIncomingProbeMapping =
      new MappingSet("incomingRowIdx", null, "incomingProbe", null, DO_SETUP_CONSTANT, GET_HASH_PROBE);
  private final MappingSet SetValueMapping =
      new MappingSet("incomingRowIdx" /* read index */, "htRowIdx" /* write index */,
          "incomingBuild" /* read container */, "htContainer" /* write container */, SETUP_INTERIOR_CONSTANT,
          SET_VALUE);

  private final MappingSet OutputRecordKeysMapping =
      new MappingSet("htRowIdx" /* read index */, "outRowIdx" /* write index */, "htContainer" /* read container */,
          "outgoing" /* write container */, SETUP_INTERIOR_CONSTANT, OUTPUT_KEYS);

  private HashTableConfig htConfig;
  private final BufferAllocator allocator;
  private final VectorAccessible incomingBuild;
  private final VectorAccessible incomingProbe;
  private final VectorAccessible outgoing;
  private final ClassProducer producer;
  private final BatchAddedListener listener;

  public ChainedHashTable(
      HashTableConfig htConfig,
      ClassProducer producer,
      BufferAllocator allocator,
      VectorAccessible incomingBuild,
      VectorAccessible incomingProbe,
      VectorAccessible outgoing,
      BatchAddedListener listener) {

    this.htConfig = htConfig;
    this.producer = producer;
    this.allocator = allocator;
    this.incomingBuild = incomingBuild;
    this.incomingProbe = incomingProbe;
    this.outgoing = outgoing;
    this.listener = listener;
  }

  public HashTable createAndSetupHashTable(TypedFieldId[] outKeyFieldIds) throws ClassTransformationException,
      IOException, SchemaChangeException {
    final CodeGenerator<HashTable> codeGenerator = producer.createGenerator(HashTable.TEMPLATE_DEFINITION);
    final ClassGenerator<HashTable> hashTableGen = codeGenerator.getRoot();
    final ClassGenerator<HashTable> batchHolderGen = hashTableGen.getInnerGenerator("BatchHolder");

    final LogicalExpression[] keyExprsBuild = new LogicalExpression[htConfig.getKeyExprsBuild().size()];
    final LogicalExpression[] keyExprsProbe;
    final boolean isProbe = (htConfig.getKeyExprsProbe() != null);
    if (isProbe) {
      keyExprsProbe = new LogicalExpression[htConfig.getKeyExprsProbe().size()];
    } else {
      keyExprsProbe = null;
    }


    final VectorContainer htContainerOrig = new VectorContainer(); // original ht container from which others may be cloned
    final TypedFieldId[] htKeyFieldIds = new TypedFieldId[htConfig.getKeyExprsBuild().size()];

    int i = 0;
    for (NamedExpression ne : htConfig.getKeyExprsBuild()) {
      final LogicalExpression expr = producer.materialize(ne.getExpr(), incomingBuild);
      if (expr == null) {
        continue;
      }
      keyExprsBuild[i] = expr;
      i++;
    }

    if (isProbe) {
      i = 0;
      for (NamedExpression ne : htConfig.getKeyExprsProbe()) {
        final LogicalExpression expr = producer.materialize(ne.getExpr(), incomingProbe);
        if (expr == null) {
          continue;
        }
        keyExprsProbe[i] = expr;
        i++;
      }
      JoinUtils.addLeastRestrictiveCasts(keyExprsProbe, incomingProbe, keyExprsBuild, incomingBuild, producer);
    }

    i = 0;
    /*
     * Once the implicit casts have been added, create the value vectors for the corresponding
     * type and add it to the hash table's container.
     * Note: Adding implicit casts may have a minor impact on the memory foot print. For example
     * if we have a join condition with bigint on the probe side and int on the build side then
     * after this change we will be allocating a bigint vector in the hashtable instead of an int
     * vector.
     */
    for (NamedExpression ne : htConfig.getKeyExprsBuild()) {
      LogicalExpression expr = keyExprsBuild[i];
      final Field outputField = expr.getCompleteType().toField(ne.getRef());
      ValueVector vv = TypeHelper.getNewVector(outputField, allocator);
      htKeyFieldIds[i] = htContainerOrig.add(vv);
      i++;
    }

    // generate code for isKeyMatch(), setValue(), getHash() and outputRecordKeys()
    setupIsKeyMatchInternal(batchHolderGen, KeyMatchIncomingBuildMapping, KeyMatchHtableMapping, keyExprsBuild,
        htConfig.getComparators(), htKeyFieldIds);
    setupIsKeyMatchInternal(batchHolderGen, KeyMatchIncomingProbeMapping, KeyMatchHtableProbeMapping, keyExprsProbe,
        htConfig.getComparators(), htKeyFieldIds);

    setupSetValue(batchHolderGen, keyExprsBuild, htKeyFieldIds);
    if (outgoing != null) {

      if (outKeyFieldIds != null && outKeyFieldIds.length > htConfig.getKeyExprsBuild().size()) {
        throw new IllegalArgumentException("Mismatched number of output key fields.");
      }
    }
    setupOutputRecordKeys(batchHolderGen, htKeyFieldIds, outKeyFieldIds);

    setupGetHash(hashTableGen /* use top level code generator for getHash */, GetHashIncomingBuildMapping, incomingBuild, keyExprsBuild, false);
    setupGetHash(hashTableGen /* use top level code generator for getHash */, GetHashIncomingProbeMapping, incomingProbe, keyExprsProbe, true);

    HashTable ht = codeGenerator.getImplementationClass();
    ht.setup(htConfig, producer.getFunctionContext(), allocator, incomingBuild, incomingProbe, outgoing, htContainerOrig, listener);

    return ht;
  }


  private void setupIsKeyMatchInternal(ClassGenerator<HashTable> cg, MappingSet incomingMapping, MappingSet htableMapping,
      LogicalExpression[] keyExprs, List<Comparator> comparators, TypedFieldId[] htKeyFieldIds)
      throws SchemaChangeException {
    cg.setMappingSet(incomingMapping);

    if (keyExprs == null || keyExprs.length == 0) {
      cg.getEvalBlock()._return(JExpr.FALSE);
      return;
    }

    for (int i=0; i<keyExprs.length; i++) {
      final LogicalExpression expr = keyExprs[i];
      cg.setMappingSet(incomingMapping);
      HoldingContainer left = cg.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);

      cg.setMappingSet(htableMapping);
      ValueVectorReadExpression vvrExpr = new ValueVectorReadExpression(htKeyFieldIds[i]);
      HoldingContainer right = cg.addExpr(vvrExpr, ClassGenerator.BlockCreateMode.MERGE);

      JConditional jc;

      // codegen for nullable columns if nulls are not equal
      if (comparators.get(i) == Comparator.EQUALS) {
        jc = cg.getEvalBlock()._if(left.getIsSet().eq(JExpr.lit(0)).
            cand(right.getIsSet().eq(JExpr.lit(0))));
        jc._then()._return(JExpr.FALSE);
      }

      final LogicalExpression f = FunctionGenerationHelper.getOrderingComparatorNullsHigh(left, right, producer);

      HoldingContainer out = cg.addExpr(f, ClassGenerator.BlockCreateMode.MERGE);

      // check if two values are not equal (comparator result != 0)
      jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      jc._then()._return(JExpr.FALSE);
    }

    // All key expressions compared equal, so return NEW_BLOCK
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  private void setupSetValue(ClassGenerator<HashTable> cg, LogicalExpression[] keyExprs,
                             TypedFieldId[] htKeyFieldIds) throws SchemaChangeException {

    cg.setMappingSet(SetValueMapping);

    int i = 0;
    for (LogicalExpression expr : keyExprs) {
      boolean useSetSafe = !expr.getCompleteType().isFixedWidthScalar();
      ValueVectorWriteExpression vvwExpr = new ValueVectorWriteExpression(htKeyFieldIds[i++], expr, useSetSafe);

      cg.addExpr(vvwExpr, ClassGenerator.BlockCreateMode.MERGE); // this will write to the htContainer at htRowIdx
    }
  }

  private void setupOutputRecordKeys(ClassGenerator<HashTable> cg, TypedFieldId[] htKeyFieldIds, TypedFieldId[] outKeyFieldIds) {

    cg.setMappingSet(OutputRecordKeysMapping);

    if (outKeyFieldIds != null) {
      for (int i = 0; i < outKeyFieldIds.length; i++) {
        ValueVectorReadExpression vvrExpr = new ValueVectorReadExpression(htKeyFieldIds[i]);
        boolean useSetSafe = !vvrExpr.getCompleteType().isFixedWidthScalar();
        ValueVectorWriteExpression vvwExpr = new ValueVectorWriteExpression(outKeyFieldIds[i], vvrExpr, useSetSafe);
        cg.addExpr(vvwExpr, ClassGenerator.BlockCreateMode.NEW_BLOCK);
      }

    }
  }

  private void setupGetHash(ClassGenerator<HashTable> cg, MappingSet incomingMapping, VectorAccessible batch, LogicalExpression[] keyExprs,
                            boolean isProbe) throws SchemaChangeException {

    cg.setMappingSet(incomingMapping);

    if (keyExprs == null || keyExprs.length == 0) {
      cg.getEvalBlock()._return(JExpr.lit(0));
      return;
    }

    /*
     * We use the same logic to generate run time code for the hash function both for hash join and hash
     * aggregate.
     */
    LogicalExpression hashExpression = HashPrelUtil.getHashExpression(Arrays.asList(keyExprs));
    final LogicalExpression materializedExpr = producer.materialize(hashExpression, batch);
    HoldingContainer hash = cg.addExpr(materializedExpr);
    cg.getEvalBlock()._return(hash.getValue());


  }
}
