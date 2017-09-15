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

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.quartz.JobPersistenceException;
import org.quartz.Trigger;
import org.quartz.core.SchedulingContext;
import org.quartz.impl.jdbcjobstore.JobStoreCMT;
import org.quartz.utils.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BatchJobStoreCMT extends JobStoreCMT {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchJobStoreCMT.class); 
    
    private int batchSize = Integer.parseInt(System.getProperty("org.jbpm.timer.quartz.batch.size", "50"));
    
    private LinkedBlockingQueue<Trigger> acquiredTriggers = new LinkedBlockingQueue<>();

    @Override
    public Trigger acquireNextTrigger(SchedulingContext ctxt, long noLaterThan) throws JobPersistenceException {
        if (acquiredTriggers.isEmpty()) {
            logger.debug("Loading triggers from db...");
            super.acquireNextTrigger(ctxt, noLaterThan);
            
        } else {
            logger.debug("Acquired triggers available {}", acquiredTriggers.size());
        }
    
        logger.debug("Waiting for trigger to be available...");
        Trigger trigger = acquiredTriggers.poll();
        logger.debug("Returning following trigger {}", trigger);
        return trigger;
    }

    @Override
    protected Trigger acquireNextTrigger(Connection conn, SchedulingContext ctxt, long noLaterThan) throws JobPersistenceException {
        int batchCounter = 0;
        do {
            try {
                Trigger nextTrigger = null;
                
                List keys = getDelegate().selectTriggerToAcquire(conn, noLaterThan, getMisfireTime());
                logger.debug("Found following number of triggers {}", keys.size());
                // No trigger is ready to fire yet.
                if (keys == null || keys.size() == 0)
                    return null;
                
                Iterator itr = keys.iterator();
                while(itr.hasNext()) {
                    Key triggerKey = (Key) itr.next();
                    if (batchCounter == batchSize) {
                        break;
                    }
                    int rowsUpdated = 
                        getDelegate().updateTriggerStateFromOtherState(
                            conn,
                            triggerKey.getName(), triggerKey.getGroup(), 
                            STATE_ACQUIRED, STATE_WAITING);
    
                    // If our trigger was no longer in the expected state, try a new one.
                    if (rowsUpdated <= 0) {
                        continue;
                    }
    
                    nextTrigger = 
                        retrieveTrigger(conn, ctxt, triggerKey.getName(), triggerKey.getGroup());
    
                    // If our trigger is no longer available, try a new one.
                    if(nextTrigger == null) {
                        continue;
                    }

                    
                    nextTrigger.setFireInstanceId(getFiredTriggerRecordId());
                    getDelegate().insertFiredTrigger(conn, nextTrigger, STATE_ACQUIRED, null);
                    logger.debug("Adding {} to accuried triggers", nextTrigger);
                    acquiredTriggers.offer(nextTrigger);
                    batchCounter++;
                }
                
                
                return null;
                

            } catch (Exception e) {
                throw new JobPersistenceException(
                          "Couldn't acquire next trigger: " + e.getMessage(), e);
            }
        } while (true);
    }

}
