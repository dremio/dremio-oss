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
package com.dremio.dac.explore;

import static java.util.Collections.singletonList;

import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.extract.Card;
import com.dremio.dac.explore.model.extract.MapSelection;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ExtractListRule;
import com.dremio.dac.proto.model.dataset.ExtractMapRule;
import com.dremio.dac.proto.model.dataset.ExtractRule;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.SplitRule;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.Lists;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;

/** Transform recommender. */
public class Recommenders {

  private static final Comparator<Card<ReplacePatternRule>> REPLACE_RULE_COMPARATOR =
      new Comparator<Card<ReplacePatternRule>>() {
        @Override
        public int compare(Card<ReplacePatternRule> o1, Card<ReplacePatternRule> o2) {
          int matchDiff = Integer.compare(o2.getMatchedCount(), o1.getMatchedCount());
          if (matchDiff != 0) {
            return matchDiff;
          }
          boolean ic1 = o1.getRule().getIgnoreCase() == null ? false : o1.getRule().getIgnoreCase();
          boolean ic2 = o2.getRule().getIgnoreCase() == null ? false : o2.getRule().getIgnoreCase();
          if (ic1 && !ic2) {
            return 1;
          } else if (ic2 && !ic1) {
            return -1;
          }
          return 0;
        }
      };

  private final ExtractRecommender extract;
  private final ReplaceRecommender replace;
  private final SplitRecommender split;
  private final ExtractMapRecommender extractMap;
  private final ExtractListRecommender extractList;

  private final CardGenerator cardGenerator;

  public Recommenders(
      final QueryExecutor executor, final DatasetPath datasetPath, final DatasetVersion version) {
    this.extract = new ExtractRecommender();
    this.replace = new ReplaceRecommender();
    this.split = new SplitRecommender();
    this.extractMap = new ExtractMapRecommender();
    this.extractList = new ExtractListRecommender();
    this.cardGenerator = new CardGenerator(executor, datasetPath, version);
  }

  public List<Card<ExtractRule>> recommendExtract(
      Selection selection, SqlQuery datasetSql, BufferAllocator allocator)
      throws DatasetVersionNotFoundException {

    List<TransformRuleWrapper<ExtractRule>> ruleWrappers =
        getRuleWrappers(extract, selection, DataType.TEXT);

    return cardGenerator.generateCards(
        datasetSql,
        selection.getColName(),
        ruleWrappers,
        Recommenders.<ExtractRule>genericComparator(),
        allocator);
  }

  public Card<ExtractRule> generateExtractCard(
      ExtractRule rule, String colName, SqlQuery datasetSql, BufferAllocator allocator) {
    final TransformRuleWrapper<ExtractRule> ruleWrapper = extract.wrapRule(rule);

    return cardGenerator
        .generateCards(datasetSql, colName, singletonList(ruleWrapper), null, allocator)
        .get(0);
  }

  public List<Card<ReplacePatternRule>> recommendReplace(
      Selection selection, DataType selColType, SqlQuery datasetSql, BufferAllocator allocator)
      throws DatasetVersionNotFoundException {

    final List<TransformRuleWrapper<ReplacePatternRule>> ruleWrappers =
        getRuleWrappers(replace, selection, selColType);

    final List<Card<ReplacePatternRule>> cards =
        cardGenerator.generateCards(
            datasetSql, selection.getColName(), ruleWrappers, REPLACE_RULE_COMPARATOR, allocator);

    return cards;
  }

  public Card<ReplacePatternRule> generateReplaceCard(
      ReplacePatternRule rule, String colName, SqlQuery datasetSql, BufferAllocator allocator) {
    final TransformRuleWrapper<ReplacePatternRule> ruleWrapper = replace.wrapRule(rule);

    return cardGenerator
        .generateCards(datasetSql, colName, singletonList(ruleWrapper), null, allocator)
        .get(0);
  }

  public List<Card<SplitRule>> recommendSplit(
      Selection selection, SqlQuery datasetSql, BufferAllocator allocator)
      throws DatasetVersionNotFoundException {

    List<TransformRuleWrapper<SplitRule>> ruleWrappers =
        getRuleWrappers(split, selection, DataType.TEXT);
    return cardGenerator.generateCards(
        datasetSql,
        selection.getColName(),
        ruleWrappers,
        Recommenders.<SplitRule>genericComparator(),
        allocator);
  }

  public Card<SplitRule> generateSplitCard(
      SplitRule rule, String colName, SqlQuery datasetSql, BufferAllocator allocator) {
    final TransformRuleWrapper<SplitRule> ruleWrapper = split.wrapRule(rule);

    return cardGenerator
        .generateCards(datasetSql, colName, singletonList(ruleWrapper), null, allocator)
        .get(0);
  }

  public List<Card<ExtractMapRule>> recommendExtractMap(
      MapSelection selection, SqlQuery datasetSql, BufferAllocator allocator)
      throws DatasetVersionNotFoundException {

    List<TransformRuleWrapper<ExtractMapRule>> ruleWrappers =
        getRuleWrappers(extractMap, selection, DataType.STRUCT);
    return cardGenerator.generateCards(
        datasetSql,
        selection.getColName(),
        ruleWrappers,
        Recommenders.<ExtractMapRule>genericComparator(),
        allocator);
  }

  public Card<ExtractMapRule> generateExtractMapCard(
      ExtractMapRule rule, String colName, SqlQuery datasetSql, BufferAllocator allocator) {
    final TransformRuleWrapper<ExtractMapRule> ruleWrapper = extractMap.wrapRule(rule);

    return cardGenerator
        .generateCards(datasetSql, colName, singletonList(ruleWrapper), null, allocator)
        .get(0);
  }

  public List<Card<ExtractListRule>> recommendExtractList(
      Selection selection, SqlQuery datasetSql, BufferAllocator allocator)
      throws DatasetVersionNotFoundException {

    List<TransformRuleWrapper<ExtractListRule>> ruleWrappers =
        getRuleWrappers(extractList, selection, DataType.LIST);
    return cardGenerator.generateCards(
        datasetSql,
        selection.getColName(),
        ruleWrappers,
        Recommenders.<ExtractListRule>genericComparator(),
        allocator);
  }

  public Card<ExtractListRule> generateExtractListCard(
      ExtractListRule rule, String colName, SqlQuery datasetSql, BufferAllocator allocator) {
    final TransformRuleWrapper<ExtractListRule> ruleWrapper = extractList.wrapRule(rule);

    return cardGenerator
        .generateCards(datasetSql, colName, singletonList(ruleWrapper), null, allocator)
        .get(0);
  }

  private static <T, U> List<TransformRuleWrapper<T>> getRuleWrappers(
      Recommender<T, U> recommender, U selection, DataType selColType) {
    List<T> rules = recommender.getRules(selection, selColType);

    List<TransformRuleWrapper<T>> ruleWrappers = Lists.newArrayList();
    for (T rule : rules) {
      ruleWrappers.add(recommender.wrapRule(rule));
    }

    return ruleWrappers;
  }

  private static <T> Comparator<Card<T>> genericComparator() {
    return new Comparator<Card<T>>() {
      @Override
      public int compare(Card<T> o1, Card<T> o2) {
        return Integer.compare(o2.getMatchedCount(), o1.getMatchedCount());
      }
    };
  }
}
