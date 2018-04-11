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
package com.dremio.dac.explore;

import static com.dremio.common.utils.SqlUtils.quoteIdentifier;
import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.extract.Card;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.proto.model.dataset.CardExample;
import com.dremio.dac.proto.model.dataset.CardExamplePosition;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Generated recommendation card(s) for given transform rule wrapped in {@link TransformRuleWrapper}.
 * Each card contains:
 *   <ul>
 *     <li>how many rows matching/not-matching the rule in given dataset/version sample</li>
 *     <li>at max 3 examples of {@link CardExample}</li>
 *   </ul>
 */
public class CardGenerator {

  private final QueryExecutor executor;
  private final DatasetPath datasetPath;
  private final DatasetVersion version;

  public CardGenerator(final QueryExecutor executor, DatasetPath datasetPath, DatasetVersion version) {
    this.executor = executor;
    this.datasetPath = datasetPath;
    this.version = version;
  }

  /**
   * Generate cards.
   *
   * @param datasetSql Sql of the dataset version for which cards are needed.
   * @param colName Column name on which the transform rule needs to be applied
   * @param transformRuleWrappers set of transform rule wrappers.
   * @param comparator Optional comparator to use in sorting the generated cards.
   * @param <T> transform rule
   * @return
   */
  public <T> List<Card<T>> generateCards(SqlQuery datasetSql, String colName,
      List<TransformRuleWrapper<T>> transformRuleWrappers, Comparator<Card<T>> comparator) {

    JobUI datasetPreviewJob = DatasetsUtil.getDatasetPreviewJob(executor, datasetSql, datasetPath, version);
    String previewDataTable = datasetPreviewJob.getData().getJobResultsTable();

    // Generate a query to count the number of matches for each rule and total number of rows. Input here is from
    // preview data of the dataset with version.
    String countQuery = generateMatchCountQuery(colName, previewDataTable, transformRuleWrappers);

    JobUI countJob = executor.runQuery(datasetSql.cloneWithNewSql(countQuery), QueryType.UI_INTERNAL_RUN, datasetPath, version);
    JobDataFragment countJobData = countJob.getData().truncate(1);

    // Get the total number of records
    final int totalCount = toIntOrZero(countJobData.extractValue("total", 0));

    String exGenQuery = generateCardGenQuery(colName, previewDataTable, transformRuleWrappers);

    JobUI exGenQueryJob = executor.runQuery(datasetSql.cloneWithNewSql(exGenQuery), QueryType.UI_INTERNAL_RUN, datasetPath, version);
    List<List<CardExample>> cardsExamples = getExamples(exGenQueryJob, transformRuleWrappers);

    List<Card<T>> cards = Lists.newArrayList();
    for(int i = 0; i < transformRuleWrappers.size() ; i++) {
      // Get match count for current rule
      final int matchedCount = toIntOrZero(countJobData.extractValue("matched_count_" + i, 0));

      Recommender.TransformRuleWrapper<T> evaluator = transformRuleWrappers.get(i);

      Card<T> card = new Card<>(evaluator.getRule(), cardsExamples.get(i),
          matchedCount, totalCount - matchedCount, evaluator.describe());

      cards.add(card);
    }

    if (comparator != null) {
      Collections.sort(cards, comparator);
    }

    return cards;
  }

  static int toIntOrZero(Object o) {
    // Note instanceof always returns false for null input, so this is null safe
    if (o instanceof Number) {
      return ((Number)o).intValue();
    }
    return 0;
  }

  <T> List<List<CardExample>> getExamples(JobUI exGenQueryJob, List<TransformRuleWrapper<T>> transformRuleWrappers) {

    JobDataFragment data = exGenQueryJob.getData().truncate(Card.EXAMPLES_TO_SHOW);

    final List<List<CardExample>> examples = Lists.newArrayList();
    for(int ruleIndex = 0; ruleIndex < transformRuleWrappers.size(); ruleIndex++) {
      examples.add(Lists.<CardExample>newArrayList());
    }

    for (int row = 0; row < data.getReturnedRowCount(); row++) {
      final String input = data.extractString("inputCol", row);
      for (int ruleIndex = 0; ruleIndex < transformRuleWrappers.size(); ruleIndex++) {
        if (!transformRuleWrappers.get(ruleIndex).canGenerateExamples()) {
          continue;
        }
        final String outputColAlias = "example_" + ruleIndex;
        final Object value = data.extractValue(outputColAlias, row);

        CardExample example = new CardExample(input);
        example.setPositionList(new ArrayList<CardExamplePosition>());
        if (value != null && value instanceof List<?>) {
          List<Map<?,?>> positions = (List<Map<?,?>>) value;

          if (positions.size() == 0) {
            example.getPositionList().add(new CardExamplePosition(0, 0));
          } else {
            for (Map<?, ?> position : positions) {
              final Integer offset = (Integer) position.get("offset");
              final Integer length = (Integer) position.get("length");

              example.getPositionList().add(new CardExamplePosition(offset, length));
            }
          }
        } else {
          example.getPositionList().add(new CardExamplePosition(0, 0));
        }

        examples.get(ruleIndex).add(example);
      }
    }

    return examples;
  }

  <T> String generateCardGenQuery(String inputColName, String datasetPreviewTable, List<TransformRuleWrapper<T>> evaluators) {

    StringBuilder queryBuilder = new StringBuilder();

    String inputExpr = String.format("%s.%s", quoteIdentifier("dremio_preview_data"), quoteIdentifier(inputColName));
    List<String> exprs = Lists.newArrayList();
    for(int i=0; i<evaluators.size(); i++) {
      if (evaluators.get(i).canGenerateExamples()) {
        final String expr = evaluators.get(i).getExampleFunctionExpr(inputExpr);
        final String outputColAlias = "example_" + i;

        exprs.add(String.format("%s AS %s", expr, outputColAlias));
      }
    }

    exprs.add(String.format("%s AS inputCol", inputExpr));

    queryBuilder.append("SELECT\n");

    queryBuilder.append(Joiner.on(",\n").join(exprs));

    queryBuilder.append(format("\nFROM %s as dremio_preview_data", datasetPreviewTable));

    queryBuilder.append(format("\nWHERE %s IS NOT NULL", quoteIdentifier(inputColName)));

    queryBuilder.append(format("\nLIMIT %d", Card.EXAMPLES_TO_SHOW));

    return queryBuilder.toString();
  }

  <T> String generateMatchCountQuery(String inputColName, String datasetPreviewTable, List<TransformRuleWrapper<T>> evaluators) {

    StringBuilder queryBuilder = new StringBuilder();

    String inputExpr = String.format("%s.%s", quoteIdentifier("dremio_preview_data"), quoteIdentifier(inputColName));
    List<String> exprs = Lists.newArrayList();
    for(int i=0; i<evaluators.size(); i++) {
      final String expr = evaluators.get(i).getMatchFunctionExpr(inputExpr);

      final String outputColAlias = "matched_count_" + i;

      // Add sum over the true or false expression
      exprs.add(String.format("sum(CASE WHEN %s THEN 1 ELSE 0 END) AS %s", expr, outputColAlias));
    }

    // Add an count(*) to count the total number of rows in job output.
    // This was changed for a previous use of sum(1), as this produces null for an empty input set
    // which we can have if our sample fails all filters and in other cases
    exprs.add("COUNT(1) as total");

    queryBuilder.append("SELECT\n");

    queryBuilder.append(Joiner.on(",\n").join(exprs));

    queryBuilder.append(format("\nFROM %s as dremio_preview_data", datasetPreviewTable));

    return queryBuilder.toString();
  }
}
