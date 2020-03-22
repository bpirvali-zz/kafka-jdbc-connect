/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class BatchIdTableQuerier extends TimestampIncrementingTableQuerier {
  private static final Logger log = LoggerFactory.getLogger(BatchIdTableQuerier.class);

  public static final String TEMPLATE_SRC_TABLE_NAME = "__TABLE_NAME__";
  public static final String TEMPLATE_OFFSET = "__OFFSET__";
  private final String srcTableName;
  private final String batchType;
  private TimestampIncrementingOffset oldOffset;
  private final BatchIdManager batchIdManager;

  // pre-run-check values
  private Timestamp runToOffsetTs = null;
  private Long runToOffsetLong = null;
  private String preQuerySql = null;

  private final BatchModeInfo batchModeInfo;
  private final Connection conn;

  public static class BatchModeInfo {
    public final CachedConnectionProvider cachedConnectionProvider;
    public final DatabaseDialect dialect;
    public final String preQuery;
    public final String offsetsTableName ;
    public final String strStartOffset;
    public final String tableOrQuery;
    public final List<String> timestampColumns;
    public final String timestampColumn;
    public final String incrementingColumn;

    public BatchModeInfo(CachedConnectionProvider connectionProvider,
                         DatabaseDialect dialect,
                         String preQuery,
                         String offsetTableName,
                         String strStartOffset,
                         String tableOrQuery,
                         List<String> timestampColumns,
                         String incrementingColumn) {
      this.cachedConnectionProvider = connectionProvider;
      this.dialect = dialect;
      this.preQuery = preQuery;
      this.offsetsTableName = offsetTableName;
      this.strStartOffset = strStartOffset;
      this.tableOrQuery = tableOrQuery;
      this.timestampColumns = timestampColumns;
      this.incrementingColumn = incrementingColumn;

      if (timestampColumns != null && timestampColumns.size() > 1) {
        throw new ConnectException("batch-mode does not support multilple timestamp columns");
      }
      this.timestampColumn = (timestampColumns != null && timestampColumns.size() > 0)
              ? timestampColumns.get(0) : null;
    }
  }

  public static BatchIdManager createBatchIdManager(
          BatchIdTableQuerier.BatchModeInfo batchModeInfo,
          DatabaseDialect dialect,
          List<String> timestampColumnNames,
          String topicPrefix,
          String tableOrQuery) {


    // srcTableName
    TableId tableId = dialect.parseTableIdentifier(tableOrQuery);
    String srcTableName = tableId.schemaName() + "." + tableId.tableName();

    // batch-type
    String batchType = getBatchType(timestampColumnNames);

    // check start offset
    // checkStartOffset(batchType, batchModeInfo.strStartOffset);

    // TODO: BP: 2020-03-13 If storage = "kafka.<topic-name> ==> create a kafka-batch-manager
    return new JdbcBatchIdManager(batchModeInfo, dialect,
            batchType, topicPrefix, srcTableName);
  }

  private static String getBatchType(List<String> timestampColumnNames) {
    if (timestampColumnNames == null || timestampColumnNames.size() == 0) {
      return BatchIdManager.BATCH_TYPE_LONG;
    }
    return BatchIdManager.BATCH_TYPE_TIMESTAMP;
  }

  public BatchIdTableQuerier(BatchModeInfo batchModeInfo,
                             QueryMode mode, BatchIdManager batchIdManager,
                             String topicPrefix, List<String> timestampColumnNames,
                             String incrementingColumnName, Map<String, Object> offsetMap,
                             Long timestampDelay, TimeZone timeZone, String suffix) {
    super(batchModeInfo.dialect, mode, batchModeInfo.tableOrQuery, topicPrefix,
            batchModeInfo.timestampColumns, batchModeInfo.incrementingColumn,
            offsetMap, timestampDelay, timeZone, suffix);

    // SQL connection
    conn = batchModeInfo.cachedConnectionProvider.getConnection();
    try {
      conn.setAutoCommit(true);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    } catch (SQLException e) {
      throw new ConnectException("Exception in BatchIdTableQuerier", e);
    }

    this.batchModeInfo = batchModeInfo;

    // Batch-id can either be timestamp or id
    boolean validTsColumnNames = (timestampColumnNames != null && timestampColumnNames.size() > 0);
    assert (incrementingColumnName == null && validTsColumnNames)
            || (incrementingColumnName != null && !validTsColumnNames);

    // tableName
    srcTableName = tableId.schemaName() + "." + tableId.tableName();

    batchType = getBatchType(timestampColumnNames);

    this.batchIdManager = batchIdManager;

    // set initial offset
    offset = getInitialoffset();
  }

  private TimestampIncrementingOffset getInitialoffset() {
    return new TimestampIncrementingOffset(batchIdManager.getLastOffsetTimestamp(),
            batchIdManager.getLastOffsetLong());
  }

  private void setBatchId(TimestampIncrementingOffset oldOffset,
                          TimestampIncrementingOffset newOffset) {
    //    if (oldOffset == null) {
    //      return;
    //    }
    switch (batchType) {
      case BatchIdManager.BATCH_TYPE_LONG:
        long oldId = oldOffset.getIncrementingOffset();
        long newId = newOffset.getIncrementingOffset();
        if (oldId != batchIdManager.getStartOffsetLong() && oldId != newId) {
          batchIdManager.setLastOffsetLong(oldId);
        }
        break;

      case BatchIdManager.BATCH_TYPE_TIMESTAMP:
        Timestamp oldTS = oldOffset.getTimestampOffset();
        long oldLong = oldTS.getTime();
        long newLong = newOffset.getTimestampOffset().getTime();
        log.info("setBatchId: old-offset: {}, new-offset: {}",
                oldOffset.getTimestampOffset(), newOffset.getTimestampOffset());
        if (oldLong !=  batchIdManager.getStartOffsetTs().getTime() && oldLong != newLong) {
          batchIdManager.setLastOffsetTimestamp(oldTS);
        }
        break;

      default:
        assert false;
    }
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = new Struct(schemaMapping.schema());
    for (SchemaMapping.FieldSetter setter : schemaMapping.fieldSetters()) {
      try {
        setter.setField(record, resultSet);
      } catch (IOException e) {
        log.warn("Error mapping fields into Connect record", e);
        throw new ConnectException(e);
      } catch (SQLException e) {
        log.warn("SQL error mapping fields into Connect record", e);
        throw new DataException(e);
      }
    }
    oldOffset = offset;
    offset = getCriteria().extractValues(schemaMapping.schema(), record, offset);
    // -------------------------------------------
    // BP: 2020-03-07 15:54:02
    // -------------------------------------------
    // Sending the batch-id to be set in case of a change
    log.info("extractRecord: old-offset:{}, new-offset:{}",
            oldOffset != null ? oldOffset.getTimestampOffset() : "null",
            offset != null ? offset.getTimestampOffset() : "null");
    setBatchId(oldOffset, offset);
    oldOffset = offset;
    return new SourceRecord(getPartition(), null, getTopic(), record.schema(), record);
  }

  @Override
  public Timestamp beginTimetampValue() {
    return batchIdManager.getLastOffsetTimestamp();
  }

  @Override
  public Timestamp endTimetampValue()  throws SQLException {
    return runToOffsetTs;
    //return new Timestamp(runToOffsetTs.getTime() + 1);
  }

  @Override
  public Long lastIncrementedValue() {
    return batchIdManager.getLastOffsetLong();
  }

  @Override
  public Long higestIncrementedValue() {
    return runToOffsetLong;
  }

  @Override
  public boolean doPostProcessing() {
    boolean lastCompletedOffsetIsSet =  setLastCompletedOffsetInDB();
    if (lastCompletedOffsetIsSet) {
      log.info("-----------------------------------------------------");
      log.info("Done processing the batch! ");
      log.info("-----------------------------------------------------");
      return true;
    }
    return false;
  }

  @Override
  public boolean doPreProcessing() {
    return checkPreRun();
  }

  // -------------------------------------------
  // setLastCompletedOffsetInDB
  // -------------------------------------------
  private boolean setLastCompletedOffsetInDB() {
    boolean processed  = false;
    switch (batchType) {
      case BatchIdManager.BATCH_TYPE_LONG:
        processed = setLastOffsetLong();
        break;

      case BatchIdManager.BATCH_TYPE_TIMESTAMP:
        processed = setLastOffsetTimestamp();
        break;

      default:
        assert false;
    }
    if (!processed) {
      log.warn("setLastCompletedOffsetInDB failed to set the last completed "
              + "offset into offset-table!");
      log.warn("Probably Load_status's RunTo-offset is higher "
              + "than offset of last row in the table!");
      log.warn("runToOffsetTs: {}, runToOffsetLong: {}", runToOffsetTs, runToOffsetLong);
    }
    return processed;
  }

  //  private boolean isAnyDataReadFromDB() {
  //    assert !(offset != null && oldOffset == null) : "!(offset != null && oldOffset == null)";
  //    assert !(offset == null && oldOffset != null) : "!(offset == null && oldOffset != null)";
  //
  //    return !(offset == null & oldOffset == null);
  //  }

  private boolean setLastOffsetTimestamp() {
    //    if (!isAnyDataReadFromDB()) {
    //      return false;
    //    }
    Timestamp tsLastOffset = oldOffset.getTimestampOffset();
    if (runToOffsetTs != null && runToOffsetTs.equals(tsLastOffset)) {
      batchIdManager.setLastOffsetTimestamp(tsLastOffset);
      runToOffsetTs = null;

      return true;
    }
    return false;
  }

  private boolean setLastOffsetLong() {
    //    if (!isAnyDataReadFromDB()) {
    //      return false;
    //    }
    Long lastOffset = oldOffset.getIncrementingOffset();
    if (runToOffsetLong != null && runToOffsetLong.equals(lastOffset)) {
      batchIdManager.setLastOffsetLong(lastOffset);
      runToOffsetLong = null;
      return true;
    }
    return false;
  }
  // -------------------------------------------
  // / setLastCompletedOffsetInDB
  // -------------------------------------------

  // -------------------------------------------
  // checkPreRun
  // -------------------------------------------
  /**
   *
   * @return true means run, false means wait
   */
  private boolean checkPreRun() {
    // if no query ==> we will run
    if (batchModeInfo.preQuery.trim().length() == 0) {
      return true;
    }

    // if we have a value to run to ==> we will run
    if (runToOffsetLong != null || runToOffsetTs != null) {
      return true;
    }

    // since we did not have a value ==> try to set a value
    setRunToOffset();

    // check again
    return (runToOffsetLong != null || runToOffsetTs != null);
  }

  private void setRunToOffset() {
    //Timestamp tsRunTo = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    boolean checkRunToOffset = false;
    String offsetType = null;
    try {
      stmt = getQueryStatement();
      rs = stmt.executeQuery();
      if (rs != null && rs.next()) {
        switch (batchType) {
          case BatchIdManager.BATCH_TYPE_TIMESTAMP:
            runToOffsetTs = rs.getTimestamp(1);
            offsetType = "runToOffsetTs: " + runToOffsetTs.toString();
            break;
          case BatchIdManager.BATCH_TYPE_LONG:
            runToOffsetLong = rs.getLong(1);
            offsetType = "runToOffsetLong: " + runToOffsetLong.toString() ;
            break;
          default:
            assert false;
        }
        checkRunToOffset = batchIdManager.checkRunToOffset(runToOffsetTs, runToOffsetLong);
        if (!checkRunToOffset) {
          throw new ConnectException(offsetType + " from Load-status "
                  + "table does not exist in the source table: '" + srcTableName + "'!");
        }
      } else {
        log.info("--------------------------------------------------------------------------");
        log.info("pre-query: '{}' is not returning any data", preQuerySql);
        log.info("--------------------------------------------------------------------------");
      }
    } catch (SQLException e) {
      log.error("Exception in setRunToOffset", e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        log.error("Exception closing ResultSet in setRunToOffset", e);
        throw new ConnectException(e.getMessage(), e);
      }
    }
  }

  private PreparedStatement getQueryStatement() {
    preQuerySql = batchModeInfo.preQuery.trim();
    if (preQuerySql.length() == 0) {
      return null;
    }

    String tableName = "'" + srcTableName + "'";
    preQuerySql = preQuerySql.replaceAll(TEMPLATE_SRC_TABLE_NAME, tableName);
    switch (batchType) {
      case BatchIdManager.BATCH_TYPE_LONG:
        Long runFromLong = batchIdManager.getLastOffsetLong();
        preQuerySql = preQuerySql.replaceAll(TEMPLATE_OFFSET, "" + runFromLong);
        break;

      case BatchIdManager.BATCH_TYPE_TIMESTAMP:
        Timestamp runFromTs =  batchIdManager.getLastOffsetTimestamp();
        String pattern = "yyyy-MM-dd HH:mm:ss";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        String strTs = "'" + formatter.format(runFromTs.toLocalDateTime()) + "'";

        preQuerySql = preQuerySql.replaceAll(TEMPLATE_OFFSET, strTs);
        break;

      default:
        assert false;
    }
    try {
      log.info("pre-run-query: {}", preQuerySql);
      return dialect.createPreparedStatement(conn, preQuerySql);
    } catch (SQLException e) {
      log.error("Exception in getQueryStatement", e);
      throw new ConnectException(e.getMessage(), e);
    }
    //return null;
  }
  // -------------------------------------------
  // / checkPreRun
  // -------------------------------------------

  @Override
  public String toString() {
    return "BatchIdTableQuerier{"
            + "table=" + tableId
            + ", query='" + query + '\''
            + ", topicPrefix='" + topicPrefix + '\''
            + ", incrementingColumn='" + (getIncrementingColumnName() != null
            ? getIncrementingColumnName()
            : "") + '\''
            + ", timestampColumns=" + getTimestampColumnNames()
            + '}';
  }

}
