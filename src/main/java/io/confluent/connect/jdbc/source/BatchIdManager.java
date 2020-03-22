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

import java.sql.Timestamp;

public interface BatchIdManager {
  public static final String BATCH_TYPE_TIMESTAMP = "timestamp";
  public static final String BATCH_TYPE_LONG = "long";
  //public static final Timestamp FIRST_BATCH_TS = new Timestamp(0);

  public Long getLastOffsetLong();

  public Timestamp getLastOffsetTimestamp();

  public boolean checkRunToOffset(Timestamp runToOffsetTs, Long runToOffsetLong);

  public void setLastOffsetLong(long lastOffset);

  public void setLastOffsetTimestamp(Timestamp lastOffset);

  public Timestamp getStartOffsetTs();

  public Long getStartOffsetLong();
}
