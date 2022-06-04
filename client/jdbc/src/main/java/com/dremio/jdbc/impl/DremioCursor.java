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
package com.dremio.jdbc.impl;

import static org.slf4j.LoggerFactory.getLogger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.util.ArrayImpl.Factory;
import org.apache.calcite.avatica.util.Cursor;
import org.slf4j.Logger;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserProtos.PreparedStatement;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.jdbc.SchemaChangeListener;
import com.dremio.jdbc.SqlTimeoutException;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;


class DremioCursor implements Cursor {

  /** Size of JDBC batch queue (in batches) above which throttling begins. */
  public static final String JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD = "dremio.jdbc.batch_queue_throttling_threshold";
  public static final String IS_CATALOG_NAME = "DREMIO";
  // END_OF_STREAM_MESSAGE gets added to batchQueue to signal the waiting threads
  // that there is no more data in the queue, therefore aborting the operations waiting
  // on the queue early, rather than waiting for the timeout to abort the operation.
  // We cannot have null as the END_OF_STREAM_MESSAGE as adding a null to LinkedBlockingDeque
  // throws a NullPointerException.
  public static final QueryDataBatch END_OF_STREAM_MESSAGE = new QueryDataBatch(null, null);

  ////////////////////////////////////////
  // ResultsListener:
  static class ResultsListener implements UserResultsListener {
    private static final org.slf4j.Logger logger =
        org.slf4j.LoggerFactory.getLogger(ResultsListener.class);

    private static volatile int nextInstanceId = 1;

    /** (Just for logging.) */
    private final int instanceId;

    private final int batchQueueThrottlingThreshold;

    /** (Just for logging.) */
    private volatile QueryId queryId;

    /** (Just for logging.) */
    private int lastReceivedBatchNumber;
    /** (Just for logging.) */
    private int lastDequeuedBatchNumber;

    private volatile UserException executionFailureException;

    // Completed means a state of ResultListener where we have
    // received all the data from the server.
    volatile boolean completed = false;

    /** Whether throttling of incoming data is active. */
    private final AtomicBoolean throttled = new AtomicBoolean( false );
    private volatile ConnectionThrottle throttle;

    private volatile boolean closed = false;

    private final CountDownLatch firstMessageReceived = new CountDownLatch(1);

    final LinkedBlockingDeque<QueryDataBatch> batchQueue =
        Queues.newLinkedBlockingDeque();

    private final long batchQueuePollTimeoutMs;

    // time (as epoch in millis) the query should complete before
    private long shouldCompleteBefore = Long.MAX_VALUE;

    /**
     * ...
     * @param  batchQueueThrottlingThreshold
     *         queue size threshold for throttling server
     * @param  batchQueuePollTimeoutMs
     *         timeout for batchQueue.Poll() in ms
     */
    @VisibleForTesting
    ResultsListener( int batchQueueThrottlingThreshold, long batchQueuePollTimeoutMs ) {
      instanceId = nextInstanceId++;
      this.batchQueueThrottlingThreshold = batchQueueThrottlingThreshold;
      this.batchQueuePollTimeoutMs = batchQueuePollTimeoutMs;
      logger.debug( "[#{}] Query listener created.", instanceId );
    }

    /**
     * ...
     * @param  batchQueueThrottlingThreshold
     *         queue size threshold for throttling server
     */
    ResultsListener( int batchQueueThrottlingThreshold ) {
      this(batchQueueThrottlingThreshold, 50);
    }

    /**
     * Starts throttling if not currently throttling.
     * @param  throttle  the "throttlable" object to throttle
     * @return  true if actually started (wasn't throttling already)
     */
    private boolean startThrottlingIfNot( ConnectionThrottle throttle ) {
      final boolean started = throttled.compareAndSet( false, true );
      if ( started ) {
        this.throttle = throttle;
        throttle.setAutoRead(false);
      }
      return started;
    }

    /**
     * Stops throttling if currently throttling.
     * @return  true if actually stopped (was throttling)
     */
    private boolean stopThrottlingIfSo() {
      final boolean stopped = throttled.compareAndSet( true, false );
      if ( stopped ) {
        throttle.setAutoRead(true);
        throttle = null;
      }
      return stopped;
    }

    public void awaitFirstMessage() throws TimeoutException, InterruptedException {
      if (shouldCompleteBefore == Long.MAX_VALUE) {
        firstMessageReceived.await();
      } else {
        long remaining = shouldCompleteBefore - System.currentTimeMillis();
        if (!firstMessageReceived.await(remaining, TimeUnit.MILLISECONDS)) {
          throw new TimeoutException("Did not receive first message before timeout expiration.");
        }
      }

    }

    private void releaseIfFirst() {
      firstMessageReceived.countDown();
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      logger.debug( "[#{}] Received query ID: {}.",
                    instanceId, QueryIdHelper.getQueryId( queryId ) );
      this.queryId = queryId;
    }

    @Override
    public void submissionFailed(UserException ex) {
      logger.debug( "Received query failure:", instanceId, ex );
      this.executionFailureException = ex;
      completed = true;
      close();
      logger.info( "[#{}] Query failed: ", instanceId, ex );
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      lastReceivedBatchNumber++;
      logger.debug( "[#{}] Received query data batch #{}: {}.",
                    instanceId, lastReceivedBatchNumber, result );

      // If we're in a closed state, just release the message.
      if (closed) {
        result.release();
        // TODO:  Revisit member completed:  Is ResultListener really completed
        // after only one data batch after being closed?
        completed = true;
        return;
      }

      // We're active; let's add to the queue.
      batchQueue.add(result);

      // Throttle server if queue size has exceed threshold.
      if (batchQueue.size() > batchQueueThrottlingThreshold ) {
        if ( startThrottlingIfNot( throttle ) ) {
          logger.debug( "[#{}] Throttling started at queue size {}.",
                        instanceId, batchQueue.size() );
        }
      }

      releaseIfFirst();
    }

    @Override
    public void queryCompleted(QueryState state) {
      logger.debug( "[#{}] Received query completion: {}.", instanceId, state );
      completed = true;
      // Add an END_OF_STREAM_MESSAGE batch to the queue to signify no more data.
      batchQueue.add(END_OF_STREAM_MESSAGE);
      releaseIfFirst();
    }

    QueryId getQueryId() {
      return queryId;
    }


    /**
     * Gets the next batch of query results from the queue.
     * @return  the next batch, or {@code null} after last batch has been returned
     * @throws UserException
     *         if the query failed
     * @throws TimeoutException
     *         if data was not received before timeout expiration
     * @throws InterruptedException
     *         if waiting on the queue was interrupted
     */
    QueryDataBatch getNext() throws UserException, TimeoutException, InterruptedException {
      while (true) {
        if (executionFailureException != null) {
          logger.debug( "[#{}] Dequeued query failure exception: {}.",
                        instanceId, executionFailureException );
          throw executionFailureException;
        }
        if (completed && batchQueue.isEmpty()) {
          return null;
        } else {
          long remaining = shouldCompleteBefore - System.currentTimeMillis();
          if (remaining < 0) {
            throw new TimeoutException("Query did not complete before timeout expiration");
          }
          final QueryDataBatch qdb = completed ? batchQueue.poll() :
            batchQueue.poll(Math.min(remaining, batchQueuePollTimeoutMs), TimeUnit.MILLISECONDS);
          if (qdb == END_OF_STREAM_MESSAGE) {
            return null;
          }
          if (qdb != null) {
            lastDequeuedBatchNumber++;
            logger.debug( "[#{}] Dequeued query data batch #{}: {}.",
                          instanceId, lastDequeuedBatchNumber, qdb );

            // Unthrottle server if queue size has dropped enough below threshold:
            if ( batchQueue.size() < batchQueueThrottlingThreshold / 2
                 || batchQueue.size() == 0  // (in case threshold < 2)
                 ) {
              if ( stopThrottlingIfSo() ) {
                logger.debug( "[#{}] Throttling stopped at queue size {}.",
                              instanceId, batchQueue.size() );
              }
            }
            return qdb;
          }
        }
      }
    }

    void setShouldCompleteBefore(long shouldCompleteBefore) {
      this.shouldCompleteBefore = shouldCompleteBefore;
    }

    void close() {
      logger.debug( "[#{}] Query listener closing.", instanceId );
      closed = true;
      if ( stopThrottlingIfSo() ) {
        logger.debug( "[#{}] Throttling stopped at close() (at queue size {}).",
                      instanceId, batchQueue.size() );
      }
      while (!batchQueue.isEmpty()) {
        QueryDataBatch qdb = batchQueue.poll();
        // This correctly skips over the END_OF_STREAM_MESSAGE as it has null data.
        if (qdb != null && qdb.getData() != null) {
          qdb.getData().close();
        }
      }

      completed = true;
      // Add an END_OF_STREAM_MESSAGE batch to the queue to signify no more data in a race condition
      // when the app has two threads, one that is calling ResultSet.next(), and another one where
      // the app is calling Statement.cancel(), the call to next() won't get interrupted as the
      // next() thread will have already entered the wait on the queue before seeing the cancel
      // has been requested.
      batchQueue.add(END_OF_STREAM_MESSAGE);

      // Close may be called before the first result is received and therefore
      // when the main thread is blocked waiting for the result.  In that case
      // we want to unblock the main thread.
      releaseIfFirst();
    }

  }

  private static final Logger logger = getLogger( DremioCursor.class );

  /** JDBC-specified string for unknown catalog, schema, and table names. */
  private static final String UNKNOWN_NAME_STRING = "";

  private final DremioConnectionImpl connection;
  private final AvaticaStatement statement;
  private final Meta.Signature signature;

  /** Holds current batch of records (none before first load). */
  private final RecordBatchLoader currentBatchHolder;

  private final ResultsListener resultsListener;
  private SchemaChangeListener changeListener;

  private final DremioAccessorList accessors = new DremioAccessorList();

  /** Schema of current batch (null before first load). */
  private BatchSchema schema;

  /** ... corresponds to current schema. */
  private DremioColumnMetaDataList columnMetaDataList;

  /** Whether loadInitialSchema() has been called. */
  private boolean initialSchemaLoaded = false;

  /** Whether after first batch.  (Re skipping spurious empty batches.) */
  private boolean afterFirstBatch = false;

  /**
   * Whether the next call to {@code this.}{@link #next()} should just return
   * {@code true} rather than calling {@link #nextRowInternally()} to try to
   * advance to the next record.
   * <p>
   *   Currently, can be true only for first call to {@link #next()}.
   * </p>
   * <p>
   *   (Relates to {@link #loadInitialSchema()}'s calling
   *   {@link #nextRowInternally()} one "extra" time (extra relative to number
   *   of {@link ResultSet#next()} calls) at the beginning to get first batch
   *   and schema before {@code Statement.execute...(...)} even returns.)
   * </p>
   */
  private boolean returnTrueForNextCallToNext = false;

  /** Whether cursor is after the end of the sequence of records/rows. */
  private boolean afterLastRow = false;

  private int currentRowNumber = -1;
  /** Zero-based offset of current record in record batch.
   * (Not <i>row</i> number.) */
  private int currentRecordNumber = -1;


  /**
   *
   * @param statement
   * @param signature
   */
  DremioCursor(DremioConnectionImpl connection, AvaticaStatement statement, Signature signature) {
    this.connection = connection;
    this.statement = statement;
    this.signature = signature;

    DremioClient client = connection.getClient();
    final int batchQueueThrottlingThreshold =
        client.getConfig().getInt(JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD );
    resultsListener = new ResultsListener(batchQueueThrottlingThreshold);
    currentBatchHolder = new RecordBatchLoader(client.getRecordAllocator());
  }

  protected int getCurrentRecordNumber() {
    return currentRecordNumber;
  }

  public String getQueryId() {
    if (resultsListener.getQueryId() != null) {
      return QueryIdHelper.getQueryId(resultsListener.getQueryId());
    } else {
      return null;
    }
  }

  public boolean isBeforeFirst() {
    return currentRowNumber < 0;
  }

  public boolean isAfterLast() {
    return afterLastRow;
  }

  // (Overly restrictive Avatica uses List<Accessor> instead of List<? extends
  // Accessor>, so accessors/DremioAccessorList can't be of type
  // List<SqlAccessorWrapper>, and we have to cast from Accessor to
  // SqlAccessorWrapper in updateColumns().)
  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> types,
                                        Calendar localCalendar, Factory factory) {
    columnMetaDataList = (DremioColumnMetaDataList) types;
    return accessors;
  }

  synchronized void cleanup() {
    if (resultsListener.getQueryId() != null && ! resultsListener.completed) {
      connection.getClient().cancelQuery(resultsListener.getQueryId());
    }
    resultsListener.close();
    currentBatchHolder.clear();
  }

  /**
   * Updates column accessors and metadata from current record batch.
   */
  private void updateColumns() {
    // First update accessors and schema from batch:
    accessors.generateAccessors(this, currentBatchHolder, this.connection.getTimeZone());

    // Extract Java types from accessors for metadata's getColumnClassName:
    final List<Class<?>> getObjectClasses = new ArrayList<>();
    // (Can't use modern for loop because, for some incompletely clear reason,
    // DremioAccessorList blocks iterator() (throwing exception).)
    for ( int ax = 0; ax < accessors.size(); ax++ ) {
      final SqlAccessorWrapper accessor =
          accessors.get( ax );
      getObjectClasses.add( accessor.getObjectClass() );
    }

    // Update metadata for result set.
    columnMetaDataList.updateColumnMetaData(
        IS_CATALOG_NAME,
        UNKNOWN_NAME_STRING,  // schema name
        UNKNOWN_NAME_STRING,  // table name
        schema,
        getObjectClasses );

    if (changeListener != null) {
      changeListener.schemaChanged(schema);
    }
  }

  /**
   * ...
   * <p>
   *   Is to be called (once) from {@link #loadInitialSchema} for
   *   {@link DremioResultSetImpl#execute()}, and then (repeatedly) from
   *   {@link #next()} for {@link AvaticaResultSet#next()}.
   * </p>
   *
   * @return  whether cursor is positioned at a row (false when after end of
   *   results)
   */
  private boolean nextRowInternally() throws SQLException {
    if (currentRecordNumber + 1 < currentBatchHolder.getRecordCount()) {
      // Have next row in current batch--just advance index and report "at a row."
      currentRecordNumber++;
      return true;
    } else {
      // No (more) records in any current batch--try to get first or next batch.
      // (First call always takes this branch.)

      try {
        QueryDataBatch qrb = resultsListener.getNext();

        // (Apparently:)  Skip any spurious empty batches (batches that have
        // zero rows and/or null data, other than the first batch (which carries
        // the (initial) schema but no rows)).
        if ( afterFirstBatch ) {
          while ( qrb != null
                  && ( qrb.getHeader().getRowCount() == 0
                      || qrb.getData() == null ) ) {
            // Empty message--dispose of and try to get another.
            logger.warn( "Spurious batch read: {}", qrb );

            qrb.release();

            qrb = resultsListener.getNext();
          }
        }

        afterFirstBatch = true;

        if (qrb == null) {
          // End of batches--clean up, set state to done, report after last row.

          currentBatchHolder.clear();  // (We load it so we clear it.)
          afterLastRow = true;
          return false;
        } else {
          // Got next (or first) batch--reset record offset to beginning;
          // assimilate schema if changed; set up return value for first call
          // to next().

          currentRecordNumber = 0;

          final boolean schemaChanged;
          try {
            schemaChanged = currentBatchHolder.load(qrb.getHeader().getDef(),
                                                    qrb.getData());
          }
          finally {
            qrb.release();
          }
          schema = currentBatchHolder.getSchema();
          if (schemaChanged) {
            updateColumns();
          }

          if (returnTrueForNextCallToNext
              && currentBatchHolder.getRecordCount() == 0) {
            returnTrueForNextCallToNext = false;
          }
          return true;
        }
      }
      catch ( UserException e ) {
        // A normally expected case--for any server-side error (e.g., syntax
        // error in SQL statement).
        // Construct SQLException with message text from the UserException.
        // TODO:  Map UserException error type to SQLException subclass (once
        // error type is accessible, of course. :-( )
        throw new SQLException( e.getMessage(), e );
      }
      catch ( TimeoutException e ) {
        throw new SqlTimeoutException(
            String.format("Cancelled after expiration of timeout of %d seconds.", statement.getQueryTimeout()),
            e);
      }
      catch ( InterruptedException e ) {
        // Not normally expected--Dremio doesn't interrupt in this area (right?)--
        // but JDBC client certainly could.
        throw new SQLException( "Interrupted.", e );
      }
      catch ( SchemaChangeException e ) {
        // TODO:  Clean:  DRILL-2933:  RecordBatchLoader.load(...) no longer
        // throws SchemaChangeException, so check/clean catch clause.
        throw new SQLException(
            "Unexpected SchemaChangeException from RecordBatchLoader.load(...)" );
      }
      catch ( RuntimeException e ) {
        throw new SQLException( "Unexpected RuntimeException: " + e.toString(), e );
      }

    }
  }

  /**
   * Advances to first batch to load schema data into result set metadata.
   * <p>
   *   To be called once from {@link DremioResultSetImpl#execute()} before
   *   {@link #next()} is called from {@link AvaticaResultSet#next()}.
   * <p>
   */
  void loadInitialSchema() throws SQLException {
    if ( initialSchemaLoaded ) {
      throw new IllegalStateException(
          "loadInitialSchema() called a second time" );
    }

    assert ! afterLastRow : "afterLastRow already true in loadInitialSchema()";
    assert ! afterFirstBatch : "afterLastRow already true in loadInitialSchema()";
    assert -1 == currentRecordNumber
        : "currentRecordNumber not -1 (is " + currentRecordNumber
          + ") in loadInitialSchema()";
    assert 0 == currentBatchHolder.getRecordCount()
        : "currentBatchHolder.getRecordCount() not 0 (is "
          + currentBatchHolder.getRecordCount() + " in loadInitialSchema()";

    final PreparedStatement preparedStatement;
    if (statement instanceof DremioPreparedStatementImpl) {
      DremioPreparedStatementImpl dremioPreparedStatement = (DremioPreparedStatementImpl) statement;
      preparedStatement  = dremioPreparedStatement.getPreparedStatementHandle();
    } else {
      preparedStatement = null;
    }

    long queryTimeoutSecs = statement.getQueryTimeout();
    if (queryTimeoutSecs > 0) {
      long queryEnd = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(queryTimeoutSecs);
      resultsListener.setShouldCompleteBefore(queryEnd);
    }

    if (preparedStatement != null) {
        connection.getClient().executePreparedStatement(preparedStatement.getServerHandle(), resultsListener);
    } else {
      connection.getClient().runQuery(QueryType.SQL, signature.sql, resultsListener);
    }

    try {
      resultsListener.awaitFirstMessage();
    } catch ( TimeoutException e ) {
      throw new SqlTimeoutException(
          String.format("Cancelled after expiration of timeout of %d seconds.", statement.getQueryTimeout()),
          e);
    } catch ( InterruptedException e ) {
      // Preserve evidence that the interruption occurred so that code higher up
      // on the call stack can learn of the interruption and respond to it if it
      // wants to.
      Thread.currentThread().interrupt();

      // Not normally expected--Dremio doesn't interrupt in this area (right?)--
      // but JDBC client certainly could.
      throw new SQLException("Interrupted", e );
    }

    returnTrueForNextCallToNext = true;

    nextRowInternally();

    initialSchemaLoaded = true;
  }

  /**
   * Advances this cursor to the next row, if any, or to after the sequence of
   * rows if no next row.
   *
   * @return  whether cursor is positioned at a row (false when after end of
   *   results)
   */
  @Override
  public boolean next() throws SQLException {
    if ( ! initialSchemaLoaded ) {
      throw new IllegalStateException(
          "next() called but loadInitialSchema() was not called" );
    }
    assert afterFirstBatch : "afterFirstBatch still false in next()";

    if ( afterLastRow ) {
      // We're already after end of rows/records--just report that after end.
      return false;
    }
    else if ( returnTrueForNextCallToNext ) {
      ++currentRowNumber;
      // We have a deferred "not after end" to report--reset and report that.
      returnTrueForNextCallToNext = false;
      return true;
    }
    else {
      accessors.clearLastColumnIndexedInRow();
      boolean res = nextRowInternally();
      if (res) { ++ currentRowNumber; }

      return res;
    }
  }

  public void cancel() {
    close();
  }

  @Override
  public void close() {
    // Clean up result set (to deallocate any buffers).
    cleanup();
    // TODO:  CHECK:  Something might need to set statement.openResultSet to
    // null.  Also, AvaticaResultSet.close() doesn't check whether already
    // closed and skip calls to cursor.close(), statement.onResultSetClose()
  }

  @Override
  public boolean wasNull() throws SQLException {
    return accessors.wasNull();
  }

}
