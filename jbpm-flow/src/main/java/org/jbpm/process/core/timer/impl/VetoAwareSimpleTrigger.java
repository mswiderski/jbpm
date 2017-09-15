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

import java.util.Date;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SimpleTrigger;


public class VetoAwareSimpleTrigger extends SimpleTrigger {

    private static final long serialVersionUID = 8726659476267051891L;

    public VetoAwareSimpleTrigger() {
        super();

    }

    public VetoAwareSimpleTrigger(String name, Date startTime, Date endTime, int repeatCount, long repeatInterval) {
        super(name, startTime, endTime, repeatCount, repeatInterval);

    }

    public VetoAwareSimpleTrigger(String name, Date startTime) {
        super(name, startTime);

    }

    public VetoAwareSimpleTrigger(String name, int repeatCount, long repeatInterval) {
        super(name, repeatCount, repeatInterval);

    }

    public VetoAwareSimpleTrigger(String name, String group, Date startTime, Date endTime, int repeatCount, long repeatInterval) {
        super(name, group, startTime, endTime, repeatCount, repeatInterval);

    }

    public VetoAwareSimpleTrigger(String name, String group, Date startTime) {
        super(name, group, startTime);

    }

    public VetoAwareSimpleTrigger(String name, String group, int repeatCount, long repeatInterval) {
        super(name, group, repeatCount, repeatInterval);

    }

    public VetoAwareSimpleTrigger(String name, String group, String jobName, String jobGroup, Date startTime, Date endTime, int repeatCount, long repeatInterval) {
        super(name, group, jobName, jobGroup, startTime, endTime, repeatCount, repeatInterval);

    }

    public VetoAwareSimpleTrigger(String name, String group) {
        super(name, group);

    }

    public VetoAwareSimpleTrigger(String name) {
        super(name);

    }

    @Override
    public int executionComplete(JobExecutionContext context, JobExecutionException result) {
        if (result == null) {
            return INSTRUCTION_RE_EXECUTE_JOB;
        }
        return super.executionComplete(context, result);
    }

}
