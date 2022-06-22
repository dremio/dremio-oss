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
package com.dremio.jdbc.test;

import static org.slf4j.LoggerFactory.getLogger;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;

import com.dremio.common.util.TestTools;
import com.dremio.jdbc.Driver;


/**
* Test for DRILL-1735:  Closing local JDBC connection didn't shut down
* local node to free resources (plus QueryResultBatch buffer allocation leak
* in DremioCursor.next(), lack of Metrics reset, vectors buffer leak under
* DremioCursor/DremioResultSet, and other problems).
*/
public class Bug1735ConnectionCloseTest extends JdbcTestQueryBase {
  static final Logger logger = getLogger( Bug1735ConnectionCloseTest.class );

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(120, TimeUnit.SECONDS);

  // Basic sanity test (too small to detect original connection close problem
  // but would detect QueryResultBatch release and metrics problems).

  private static final int SMALL_ITERATION_COUNT = 3;

  @Test
  public void testCloseDoesntLeakResourcesBasic() throws Exception {
    for ( int i = 1; i <= SMALL_ITERATION_COUNT; i++ ) {
      logger.info( "iteration " + i + ":" );
      System.out.println( "iteration " + i + ":" );
      Connection connection = new Driver().connect( sabotNode.getJDBCConnectionString(),
                                                    JdbcAssert.getDefaultProperties() );
      connection.close();
    }
  }


  // Test large enough to detect connection close problem (at least on
  // developer's machine).

  private static final int LARGE_ITERATION_COUNT = 1000;

  @Ignore( "Normally suppressed because slow" )
  @Test
  public void testCloseDoesntLeakResourcesMany() throws Exception {
    for ( int i = 1; i <= LARGE_ITERATION_COUNT; i++ ) {
      logger.info( "iteration " + i + ":" );
      System.out.println( "iteration " + i + ":" );

      // (Note: Can't use JdbcTest's connect(...) because it returns connection
      // that doesn't really close.
      Connection connection = new Driver().connect( sabotNode.getJDBCConnectionString(),
                                                    JdbcAssert.getDefaultProperties() );
      connection.close();
    }
  }

}
