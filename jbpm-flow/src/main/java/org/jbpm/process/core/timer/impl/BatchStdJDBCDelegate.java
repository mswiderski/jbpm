/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates.
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
package org.jbpm.process.core.timer.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;
import org.quartz.utils.Key;
import org.slf4j.Logger;


public class BatchStdJDBCDelegate extends StdJDBCDelegate {
    
    private int batchSize = Integer.parseInt(System.getProperty("org.jbpm.timer.quartz.batch.size", "50"));

    public BatchStdJDBCDelegate(Logger logger, String tablePrefix, String instanceId) {
        super(logger, tablePrefix, instanceId);
    }

    public BatchStdJDBCDelegate(Logger logger, String tablePrefix, String instanceId, Boolean useProperties) {
        super(logger, tablePrefix, instanceId, useProperties);
    }

    public List selectTriggerToAcquire(Connection conn, long noLaterThan, long noEarlierThan)
            throws SQLException {
            PreparedStatement ps = null;
            ResultSet rs = null;
            List nextTriggers = new LinkedList();
            try {
                ps = conn.prepareStatement(rtp(SELECT_NEXT_TRIGGER_TO_ACQUIRE));
                
                // Try to give jdbc driver a hint to use result size same as batch size
                ps.setFetchSize(batchSize);
                ps.setMaxRows(batchSize);
                
                ps.setString(1, STATE_WAITING);
                ps.setBigDecimal(2, new BigDecimal(String.valueOf(noLaterThan)));
                ps.setBigDecimal(3, new BigDecimal(String.valueOf(noEarlierThan)));
                rs = ps.executeQuery();
                
                while (rs.next() && nextTriggers.size() < batchSize) {
                    nextTriggers.add(new Key(
                            rs.getString(COL_TRIGGER_NAME),
                            rs.getString(COL_TRIGGER_GROUP)));
                }
                
                return nextTriggers;
            } finally {
                closeResultSet(rs);
                closeStatement(ps);
            }      
        }
}
