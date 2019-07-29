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
package com.dremio.exec.testing;

import com.dremio.common.DeferredException;
import com.dremio.exec.testing.exception.ParameterizedTestFailure;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public abstract class ParameterizedTestSuite<E> implements Testable {
  protected final boolean forgiving;
  protected final String[][] datum;
  private final DeferredException exception = new DeferredException();

  public ParameterizedTestSuite(final String[][] datum) {
    this(datum, true);
  }

  public ParameterizedTestSuite(final String[][] datum, final boolean forgiving) {
    this.datum = Preconditions.checkNotNull(datum);
    this.forgiving = forgiving;
  }

  /**
   * Builds a query against the individual test data.
   * @param data  an individual test data
   * @return sql query
   */
  protected abstract String buildQuery(final String[] data);

  /**
   * Builds a result for the individual test case.
   * @param data  an individual test data
   * @return expectation
   */
  protected abstract E buildResult(final String[] data);

  /**
   * Runs an individual test case.
   *
   * @param query  test case query
   * @param expectations  test case expectation
   */
  protected abstract void doTest(final String query, final E expectations) throws Exception;

  @Override
  public void test() throws Exception {
    for (final String[] data: datum) {
      final String query = buildQuery(data);
      final E expected = buildResult(data);
      try {
        doTest(query, expected);
      } catch (final Exception|Error ex) {
        final ParameterizedTestFailure failure = new ParameterizedTestFailure(Joiner.on(" -- ").join(data), ex);
        if (!forgiving) {
          throw failure;
        }
        exception.addException(failure);
      }
    }

    exception.throwAndClear();
  }
}
