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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

public class Test {
  private static final Logger log = LoggerFactory.getLogger(Test.class);

  private static void testPreQueryReplacement() {
    String line = "SELECT Table_Max_Td_Update_Ts FROM public.LOAD_STATUS WHERE "
            + "Table_Nm = __TABLE_NAME__ AND Table_Max_Td_Update_Ts > __OFFSET__";

    String tableName = "T1";
    // Timestamp offset = new Timestamp(System.currentTimeMillis());
    Timestamp offset = Timestamp.valueOf("1980-01-01 00:00:00");
    String pattern = "yyyy-MM-dd HH:mm:ss";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    String strTs = formatter.format(offset.toLocalDateTime());

    String nl = line.replaceAll("__TABLE_NAME__", tableName).replaceAll("__OFFSET__", strTs);
    System.out.println("line: " + nl);
  }

  private static void myLogMsg(String strFormat, Object... args) {
    log.info(strFormat, args);
  }

  private static void testPassingVarArguments() {
    Integer i = new Integer(1);
    myLogMsg("i: {}, null:{}", i, null);
  }

  public static void main(String[] args) {
  }
}
