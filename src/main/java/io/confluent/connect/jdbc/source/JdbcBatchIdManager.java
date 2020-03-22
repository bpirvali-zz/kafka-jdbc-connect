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
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Timestamp;

public class JdbcBatchIdManager implements BatchIdManager {
  private static final Logger log = LoggerFactory.getLogger(
          JdbcBatchIdManager.class
  );

  private final Connection conn;
  private final DatabaseDialect dialect;
  private final String batchType;
  private final String srcTableName;
  private final String offsetsTableName;
  private final String topicPrefix;
  private final PreparedStatement stmtInsertTable;
  private final PreparedStatement stmtCheckRunToOffset;
  private final PreparedStatement stmtGetLastCompletedBatchId;
  private final PreparedStatement stmtGetLastCompletedBatchTs;
  private final PreparedStatement stmtUpdateLastCompletedBatchId;
  private final PreparedStatement stmtUpdateLastCompletedBatchTs;
  private final Timestamp startOffsetTs;
  private final Long startOffsetLong;

  public JdbcBatchIdManager(BatchIdTableQuerier.BatchModeInfo batchModeInfo,
                            DatabaseDialect dialect,
                            String batchType,
                            String topicPrefix,
                            String srcTableName)  {
    this.conn = batchModeInfo.cachedConnectionProvider.getConnection();
    try {
      conn.setAutoCommit(true);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    } catch (SQLException e) {
      throw new ConnectException("Exception in JdbcBatchIdManager", e);
    }

    this.dialect = dialect;
    this.batchType = batchType;
    this.offsetsTableName = batchModeInfo.offsetsTableName;
    this.topicPrefix = topicPrefix;
    this.srcTableName = srcTableName;

    // ------------------------------------------------------
    // BP: 2020-03-13 11:35:47 create prepared statements
    // ------------------------------------------------------
    String strInsertTable = String.format(
            "INSERT INTO %s(topic_prefix, table_name, last_completed_ts, last_completed_id) "
            + "VALUES(?, ?, ?, ?)",
            offsetsTableName);
    String colName = (batchType == BATCH_TYPE_TIMESTAMP ? batchModeInfo.timestampColumn :
            batchModeInfo.incrementingColumn);
    String strCheckRunToOffset = String.format("SELECT CAST(1 AS INTEGER) FROM %s WHERE %s = ?",
            srcTableName, colName);

    try {
      stmtInsertTable = dialect.createPreparedStatement(conn, strInsertTable);
      stmtCheckRunToOffset = dialect.createPreparedStatement(conn, strCheckRunToOffset);
      //stmtCheckRunToOffsetLong = dialect.createPreparedStatement(conn, strCheckRunToOffsetLong);

      String strGetBatchId = String.format(
              "SELECT last_completed_id FROM %s WHERE topic_prefix = ? AND table_name = ?",
              offsetsTableName);
      stmtGetLastCompletedBatchId = dialect.createPreparedStatement(conn, strGetBatchId);

      String strGetBatchTs = String.format(
              "SELECT last_completed_ts FROM %s WHERE topic_prefix = ? AND table_name = ?",
              offsetsTableName);
      stmtGetLastCompletedBatchTs = dialect.createPreparedStatement(conn, strGetBatchTs);

      String strUpdateBatchId = String.format(
              "UPDATE %s\n"
                      + "SET \n"
                      + "    last_completed_id = ?\n"
                      + "    , update_ts = current_timestamp\n"
                      + "WHERE topic_prefix = ? AND table_name = ?\n",
              offsetsTableName);
      stmtUpdateLastCompletedBatchId = dialect.createPreparedStatement(conn, strUpdateBatchId);

      String strUpdateBatchTs = String.format(
              "UPDATE %s\n"
                      + "SET \n"
                      + "    last_completed_ts = ?\n"
                      + "    , update_ts = current_timestamp\n"
                      + "WHERE topic_prefix = ? AND table_name = ?\n",
              offsetsTableName);
      stmtUpdateLastCompletedBatchTs = dialect.createPreparedStatement(conn, strUpdateBatchTs);
      // -------------------------------------------
      // /BP: create prepared statements
      // -------------------------------------------
      setPreparedStmtsParameters();
    } catch (SQLException e) {
      log.error("Exception in JdbcBatchIdManager", e);
      throw new ConnectException("Exception in JdbcBatchIdManager", e);
    }

    // Set the start offset based on the batch-type
    final Timestamp lastOffsetTimestamp = getLastOffsetTimestamp();
    final Long lastOffsetLong = getLastOffsetLong();
    try {
      switch (batchType) {
        case BATCH_TYPE_TIMESTAMP:
          startOffsetTs = lastOffsetTimestamp != null ? lastOffsetTimestamp
                  : new Timestamp(Timestamp.valueOf(batchModeInfo.strStartOffset).getTime() - 1);
          startOffsetLong = null;
          break;

        case BATCH_TYPE_LONG:
          startOffsetLong =  lastOffsetLong != null ? lastOffsetLong
                  : new Long(Long.valueOf(batchModeInfo.strStartOffset) - 1L);;
          startOffsetTs = null;
          break;

        default:
          startOffsetTs = null;
          startOffsetLong = null;
          assert false;
          break;
      }
    } catch (Exception e) {
      throw new ConnectException("Exception in JdbcBatchIdManager", e);
    }
    createFirstRow(lastOffsetTimestamp, lastOffsetLong);
  }

  private void createFirstRow(Timestamp lastOffsetTs, Long lastOffsetLong) {
    try {
      if (lastOffsetTs == null && lastOffsetLong == null) {
        stmtInsertTable.setString(1, topicPrefix);
        stmtInsertTable.setString(2, srcTableName);
        if (startOffsetTs == null) {
          stmtInsertTable.setNull(3, Types.TIMESTAMP);
        } else {
          stmtInsertTable.setTimestamp(3, startOffsetTs);
        }
        if (startOffsetLong == null) {
          stmtInsertTable.setNull(4, Types.INTEGER);
        } else {
          stmtInsertTable.setLong(4, startOffsetLong);
        }
        stmtInsertTable.executeUpdate();
      }
    } catch (SQLException e) {
      log.error("Exception in createFirstRow", e);
      throw new ConnectException("Exception in createFirstRow", e);
    }
  }

  private void setPreparedStmtsParameters() {
    // set the parameters for get-id/ts
    try {
      stmtGetLastCompletedBatchId.setString(1, topicPrefix);
      stmtGetLastCompletedBatchId.setString(2, srcTableName);

      stmtGetLastCompletedBatchTs.setString(1, topicPrefix);
      stmtGetLastCompletedBatchTs.setString(2, srcTableName);

      // set the parameters for set-id/ts
      stmtUpdateLastCompletedBatchId.setString(2, topicPrefix);
      stmtUpdateLastCompletedBatchId.setString(3, srcTableName);

      stmtUpdateLastCompletedBatchTs.setString(2, topicPrefix);
      stmtUpdateLastCompletedBatchTs.setString(3, srcTableName);
    } catch (SQLException e) {
      log.error("Exception in setPreparedStmtsParameters", e);
      throw new ConnectException("Exception in setPreparedStmtsParameters", e);
    }
  }

  @Override
  public Long getLastOffsetLong()  {
    ResultSet rs = null;
    Long lastOffset = null;
    try {
      rs = stmtGetLastCompletedBatchId.executeQuery();
      if (rs.next()) {
        lastOffset = rs.getLong(1);
      }

    } catch (SQLException e) {
      log.error("Exception in getLastOffsetLong", e);
      throw new ConnectException("Exception in getLastOffsetLong", e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        log.error("Exception closing ResultSet in getLastOffsetTimestamp", e);
      }
    }
    return lastOffset;

    //return mapOffsetsLong.get(srcTableName);
  }

  @Override
  public Timestamp getLastOffsetTimestamp()  {
    ResultSet rs = null;
    Timestamp lastOffset = null;
    try {
      rs = stmtGetLastCompletedBatchTs.executeQuery();
      if (rs.next()) {
        lastOffset = rs.getTimestamp(1);
      }
    } catch (SQLException e) {
      log.error("Exception in getLastOffsetTimestamp", e);
      throw new ConnectException("Exception in getLastOffsetTimestamp", e);

    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        log.error("Exception closing ResultSet in getLastOffsetTimestamp", e);
      }
    }
    return lastOffset;

    // return mapOffsetsTS.get(srcTableName);
  }

  @Override
  public void setLastOffsetLong(long lastOffset) {
    // long prevOffset = mapOffsetsLong.get(srcTableName);
    long prevOffset = getLastOffsetLong();
    log.info("setLastOffsetLong: {}: {} --> {}", srcTableName, prevOffset, lastOffset);

    try {
      stmtUpdateLastCompletedBatchId.setLong(1, lastOffset);
      stmtUpdateLastCompletedBatchId.executeUpdate();
    } catch (SQLException e) {
      log.error("Exception in setLastOffsetLong", e);
      throw new ConnectException("Exception in setLastOffsetLong", e);
    }
    //mapOffsetsLong.put(srcTableName, lastOffset);
  }

  @Override
  public void   setLastOffsetTimestamp(Timestamp lastOffset) {
    // Timestamp prevOffset = mapOffsetsTS.get(srcTableName);
    Timestamp prevOffset = getLastOffsetTimestamp();
    log.info("setLastOffsetTimestamp: {}: {} --> {}", srcTableName, prevOffset, lastOffset);

    try {
      stmtUpdateLastCompletedBatchTs.setTimestamp(1, lastOffset);
      stmtUpdateLastCompletedBatchTs.setString(2, topicPrefix);
      stmtUpdateLastCompletedBatchTs.setString(3, srcTableName);
      stmtUpdateLastCompletedBatchTs.executeUpdate();
    } catch (SQLException e) {
      log.error("Exception in setLastOffsetTimestamp", e);
      throw new ConnectException("Exception in setLastOffsetTimestamp", e);
    }

    //mapOffsetsTS.put(srcTableName, lastOffset);
  }

  @Override
  public Timestamp getStartOffsetTs() {
    return startOffsetTs;
  }

  @Override
  public Long getStartOffsetLong() {
    return startOffsetLong;
  }

  @Override
  public boolean checkRunToOffset(Timestamp runToOffsetTs, Long runToOffsetLong) {
    ResultSet rs = null;
    int exist ;
    try {
      switch (batchType) {
        case BATCH_TYPE_TIMESTAMP:
          stmtCheckRunToOffset.setTimestamp(1, runToOffsetTs);
          rs = stmtCheckRunToOffset.executeQuery();
          break;

        case BATCH_TYPE_LONG:
          stmtCheckRunToOffset.setLong(1, runToOffsetLong);
          rs = stmtCheckRunToOffset.executeQuery();
          break;

        default:
          assert false;
      }
      if (rs.next()) {
        exist = rs.getInt(1);
        if (exist == 1) {
          return true;
        }
      }
    } catch (SQLException e) {
      log.error("Exception in getLastOffsetTimestamp", e);
      throw new ConnectException("Exception in getLastOffsetTimestamp", e);

    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        log.error("Exception closing ResultSet in getLastOffsetTimestamp", e);
      }
    }
    return false;
  }

}
