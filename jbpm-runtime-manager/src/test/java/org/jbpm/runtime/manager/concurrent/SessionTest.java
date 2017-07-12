/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.jbpm.runtime.manager.concurrent;

import org.jbpm.test.PoolingDataSource;
import org.drools.core.command.SingleSessionCommandService;
import org.drools.core.command.impl.CommandBasedStatefulKnowledgeSession;
import org.hibernate.StaleObjectStateException;
import org.jbpm.runtime.manager.util.TestUtil;
import org.jbpm.services.task.exception.PermissionDeniedException;
import org.jbpm.services.task.identity.JBossUserGroupCallbackImpl;
import org.jbpm.test.util.AbstractBaseTest;
import org.jbpm.workflow.instance.node.HumanTaskNodeInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.manager.RuntimeEngine;
import org.kie.api.runtime.manager.RuntimeEnvironment;
import org.kie.api.runtime.manager.RuntimeEnvironmentBuilder;
import org.kie.api.runtime.manager.RuntimeManager;
import org.kie.api.runtime.manager.RuntimeManagerFactory;
import org.kie.api.runtime.manager.audit.AuditService;
import org.kie.api.runtime.manager.audit.ProcessInstanceLog;
import org.kie.api.runtime.process.ProcessInstance;
import org.kie.api.runtime.process.WorkflowProcessInstance;
import org.kie.api.task.model.Status;
import org.kie.api.task.model.TaskSummary;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.runtime.manager.context.EmptyContext;
import org.kie.internal.runtime.manager.context.ProcessInstanceIdContext;
import org.kie.internal.task.api.UserGroupCallback;

import javax.naming.InitialContext;
import javax.persistence.OptimisticLockException;
import javax.transaction.UserTransaction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SessionTest extends AbstractBaseTest {

    private long maxWaitTime = 60*1000; // max wait to complete operation is set to 60 seconds to avoid build hangs
	
	private int nbThreadsProcess = 10;
	private int nbThreadsTask = 10;
	private int nbInvocations = 10;
	private transient int completedStart = 0;
	private transient int completedTask = 0;
	
	private PoolingDataSource pds;
	private UserGroupCallback userGroupCallback;  
	
	private RuntimeManager manager; 
	private boolean useLocking;
	
    public SessionTest(boolean locking) { 
        this.useLocking = locking; 
     }

     @Parameters
     public static Collection<Object[]> persistence() {
         Object[][] data = new Object[][] { { false } };
         return Arrays.asList(data);
     };
     
    @Before
    public void setup() {
        TestUtil.cleanupSingletonSessionId();
        Properties properties= new Properties();
        properties.setProperty("mary", "HR");
        properties.setProperty("john", "HR");
        userGroupCallback = new JBossUserGroupCallbackImpl(properties);
        
        pds = TestUtil.setupPoolingDataSource();
    }
    
    @After
    public void teardown() {
        pds.close();
        if (manager != null) {
            manager.close();
        }
        
    }

	
	@Test
	@Ignore
	public void testSingletonSessionMemory() throws Exception {
		for (int i = 0; i < 1000; i++) {
		    RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
	    			.newDefaultBuilder()
	                .userGroupCallback(userGroupCallback)
	                .addAsset(ResourceFactory.newClassPathResource("sample.bpmn"), ResourceType.BPMN2)
	                .get();
	        
	        RuntimeManager manager = RuntimeManagerFactory.Factory.get().newSingletonRuntimeManager(environment);  
	        RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
	        manager.disposeRuntimeEngine(runtime);
			manager.close();
			System.gc();
			Thread.sleep(100);
			System.gc();
			logger.info("Used memory {}", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		}
	}
	
	@Test
	public void testSingletonSession() throws Exception {
	    RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sample.bpmn"), ResourceType.BPMN2)
                .get();
	    if( useLocking ) {
	       environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
	    }
	    
        long startTimeStamp = System.currentTimeMillis();
        long maxEndTime = startTimeStamp + maxWaitTime;
        
        manager = RuntimeManagerFactory.Factory.get().newSingletonRuntimeManager(environment);  
		completedStart = 0;
		for (int i=0; i<nbThreadsProcess; i++) {
			new Thread(new StartProcessRunnable(manager, i)).start();
		}
		completedTask = 0;
		for (int i=0; i<nbThreadsTask; i++) {
			new Thread(new CompleteTaskRunnable(manager, i)).start();
		}
		while (completedStart < nbThreadsProcess || completedTask < nbThreadsTask) {
			Thread.sleep(100);
			if (System.currentTimeMillis() > maxEndTime) {
			    fail("Failure, did not finish in time most likely hanging");
			}
		}
		Thread.sleep(1000);
	      //make sure all process instance were completed
		RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
		AuditService logService = runtime.getAuditService();       
        //active
        List<? extends ProcessInstanceLog> logs = logService.findActiveProcessInstances("com.sample.bpmn.hello");
        assertNotNull(logs);
        assertEquals(0, logs.size());
        
        // completed
        logs = logService.findProcessInstances("com.sample.bpmn.hello");
        assertNotNull(logs);
        assertEquals(nbThreadsProcess*nbInvocations, logs.size());
        logger.debug("Done");
        manager.disposeRuntimeEngine(runtime);
	}
	
	@Test
	public void testNewSession() throws Exception {
	    RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sample.bpmn"), ResourceType.BPMN2)
                .get();
        if( useLocking ) {
            environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
         }
        
        long startTimeStamp = System.currentTimeMillis();
        long maxEndTime = startTimeStamp + maxWaitTime;
        
        manager = RuntimeManagerFactory.Factory.get().newPerRequestRuntimeManager(environment);
		completedStart = 0;
		for (int i=0; i<nbThreadsProcess; i++) {
			new StartProcessRunnable(manager, i).run();
		}
		completedTask = 0;
		for (int i=0; i<nbThreadsTask; i++) {
			new Thread(new CompleteTaskRunnable(manager, i)).start();
		}
		while (completedStart < nbThreadsProcess || completedTask < nbThreadsTask) {
			Thread.sleep(100);
	         if (System.currentTimeMillis() > maxEndTime) {
                fail("Failure, did not finish in time most likely hanging");
             }
		}
		//make sure all process instance were completed
		RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
		AuditService logService = runtime.getAuditService();
        
		//active
		List<? extends ProcessInstanceLog> logs = logService.findActiveProcessInstances("com.sample.bpmn.hello");
		assertNotNull(logs);
		assertEquals(0, logs.size());
		
		// completed
		logs = logService.findProcessInstances("com.sample.bpmn.hello");
        assertNotNull(logs);
        assertEquals(nbThreadsProcess*nbInvocations, logs.size());
        logger.debug("Done");
        manager.disposeRuntimeEngine(runtime);
	}
	
    @Test
    public void testSessionPerProcessInstance() throws Exception {
        RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sample.bpmn"), ResourceType.BPMN2)
                .get();
        if( useLocking ) {
            environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
         }
        
        long startTimeStamp = System.currentTimeMillis();
        long maxEndTime = startTimeStamp + maxWaitTime;
        
        manager = RuntimeManagerFactory.Factory.get().newPerProcessInstanceRuntimeManager(environment);
        completedStart = 0;
        for (int i=0; i<nbThreadsProcess; i++) {
            new StartProcessPerProcessInstanceRunnable(manager, i).run();
        }
        completedTask = 0;
        for (int i=0; i<nbThreadsTask; i++) {
            new Thread(new CompleteTaskPerProcessInstanceRunnable(manager, i)).start();
        }
        while (completedStart < nbThreadsProcess || completedTask < nbThreadsTask) {
            Thread.sleep(100);
            if (System.currentTimeMillis() > maxEndTime) {
                fail("Failure, did not finish in time most likely hanging");
            }
        }
        RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
        //make sure all process instance were completed
        AuditService logService = runtime.getAuditService();
        
        //active
        List<? extends ProcessInstanceLog> logs = logService.findActiveProcessInstances("com.sample.bpmn.hello");
        assertNotNull(logs);
        assertEquals(0, logs.size());
        
        // completed
        logs = logService.findProcessInstances("com.sample.bpmn.hello");
        assertNotNull(logs);
        assertEquals(nbThreadsProcess*nbInvocations, logs.size());        
        logger.debug("Done");
        manager.disposeRuntimeEngine(runtime);
    }
    
    @Test
    public void testNewSessionSuccess() throws Exception {
        RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sample.bpmn"), ResourceType.BPMN2)
                .get();
        if( useLocking ) {
            environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
         }
        
        manager = RuntimeManagerFactory.Factory.get().newPerRequestRuntimeManager(environment);
        RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
        KieSession ksession = runtime.getKieSession();
        UserTransaction ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
        ut.begin();
        
        ProcessInstance processInstance = ksession.startProcess("com.sample.bpmn.hello", null);
        logger.debug("Started process instance {}", processInstance.getId());
        long workItemId = ((HumanTaskNodeInstance) ((WorkflowProcessInstance) processInstance).getNodeInstances().iterator().next()).getWorkItemId();
        long taskId = runtime.getTaskService().getTaskByWorkItemId(workItemId).getId();
        runtime.getTaskService().claim(taskId, "mary");
        ut.commit();

        List<Status> statusses = new ArrayList<Status>();
        statusses.add(Status.Reserved);

        runtime = manager.getRuntimeEngine(EmptyContext.get());
        assertNotNull(runtime.getKieSession().getProcessInstance(processInstance.getId()));
        
        List<TaskSummary> tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
        assertEquals(1, tasks.size());
        
        taskId = tasks.get(0).getId();
        ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
        ut.begin();
        runtime.getTaskService().start(taskId, "mary");
        runtime.getTaskService().complete(taskId, "mary", null);
        ut.commit();
        
        assertNull(runtime.getKieSession().getProcessInstance(processInstance.getId()));
        tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
        assertEquals(0, tasks.size());
        manager.disposeRuntimeEngine(runtime);
        
        runtime = manager.getRuntimeEngine(EmptyContext.get());
        ksession = runtime.getKieSession();
        ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
        ut.begin();        
        processInstance = ksession.startProcess("com.sample.bpmn.hello", null);
        workItemId = ((HumanTaskNodeInstance) ((WorkflowProcessInstance) processInstance).getNodeInstances().iterator().next()).getWorkItemId();
        taskId = runtime.getTaskService().getTaskByWorkItemId(workItemId).getId();
        runtime.getTaskService().claim(taskId, "mary");
        logger.debug("Started process instance {}", processInstance.getId());
        ut.commit();

        assertNotNull(ksession.getProcessInstance(processInstance.getId()));
        tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
        assertEquals(1, tasks.size());

        
        taskId = tasks.get(0).getId();
        ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
        ut.begin();
        runtime.getTaskService().start(taskId, "mary");
        runtime.getTaskService().complete(taskId, "mary", null);
        ut.commit();
        
        assertNull(ksession.getProcessInstance(processInstance.getId()));
        tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
        assertEquals(0, tasks.size());
        manager.disposeRuntimeEngine(runtime);
        
    }
	
	@Test
	public void testNewSessionFail() throws Exception {
	    RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sample.bpmn"), ResourceType.BPMN2)
                .get();
        if( useLocking ) {
            environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
         }
        
        manager = RuntimeManagerFactory.Factory.get().newPerRequestRuntimeManager(environment);
        RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
        KieSession ksession = runtime.getKieSession();
		UserTransaction ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
		ut.begin();		
		ProcessInstance processInstance = ksession.startProcess("com.sample.bpmn.hello", null);
		logger.debug("Started process instance {}", processInstance.getId());
		long workItemId = ((HumanTaskNodeInstance) ((WorkflowProcessInstance) processInstance).getNodeInstances().iterator().next()).getWorkItemId();
		long taskId = runtime.getTaskService().getTaskByWorkItemId(workItemId).getId();
		runtime.getTaskService().claim(taskId, "mary");
		ut.rollback();
		logger.debug("Rolled back");
		// TODO: whenever transaction fails, do we need to dispose? can we?
		// sessionManager.dispose();

		List<Status> statusses = new ArrayList<Status>();
		statusses.add(Status.Reserved);

		runtime = manager.getRuntimeEngine(EmptyContext.get());
		assertNull(runtime.getKieSession().getProcessInstance(processInstance.getId()));
		List<TaskSummary> tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(0, tasks.size());
		
		ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
		ut.begin();
		processInstance = runtime.getKieSession().startProcess("com.sample.bpmn.hello", null);
		workItemId = ((HumanTaskNodeInstance) ((WorkflowProcessInstance) processInstance).getNodeInstances().iterator().next()).getWorkItemId();
		taskId = runtime.getTaskService().getTaskByWorkItemId(workItemId).getId();
		runtime.getTaskService().claim(taskId, "mary");
		logger.debug("Started process instance {}", processInstance.getId());
		ut.commit();

		assertNotNull(runtime.getKieSession().getProcessInstance(processInstance.getId()));
		tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(1, tasks.size());

		taskId = tasks.get(0).getId();
		ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
		ut.begin();
		runtime.getTaskService().start(taskId, "mary");
		runtime.getTaskService().complete(taskId, "mary", null);
		ut.rollback();
		manager.disposeRuntimeEngine(runtime);
		
		runtime = manager.getRuntimeEngine(EmptyContext.get());
		assertNotNull(runtime.getKieSession().getProcessInstance(processInstance.getId()));
		tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(1, tasks.size());

		taskId = tasks.get(0).getId();
		ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
		ut.begin();
		runtime.getTaskService().start(taskId, "mary");
		runtime.getTaskService().complete(taskId, "mary", null);
		ut.commit();
		
		assertNull(runtime.getKieSession().getProcessInstance(processInstance.getId()));
		tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(0, tasks.size());
		manager.disposeRuntimeEngine(runtime);
		
	}
	
	@Test
	public void testNewSessionFailBefore() throws Exception {
	    RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sampleFailBefore.bpmn"), ResourceType.BPMN2)
                .get();
        if( useLocking ) {
            environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
         }
        
        manager = RuntimeManagerFactory.Factory.get().newPerRequestRuntimeManager(environment);
        RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
		try{
			ProcessInstance processInstance = runtime.getKieSession().startProcess("com.sample.bpmn.hello", null);
			fail("Started process instance " + processInstance.getId());
		} catch (RuntimeException e) {
			// do nothing
		}

		// TODO: whenever transaction fails, do we need to dispose? can we?
		// sessionManager.dispose();
		manager.disposeRuntimeEngine(runtime);

		List<Status> statusses = new ArrayList<Status>();
		statusses.add(Status.Reserved);

		runtime = manager.getRuntimeEngine(EmptyContext.get());
		List<TaskSummary> tasks = runtime.getTaskService().getTasksAssignedAsPotentialOwner("mary", "en-UK");
		assertEquals(0, tasks.size());
		
		manager.disposeRuntimeEngine(runtime);
	}
	
	@Test
	public void testNewSessionFailAfter() throws Exception {
	    RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sampleFailAfter.bpmn"), ResourceType.BPMN2)
                .get();
        if( useLocking ) {
            environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
         }
        
        manager = RuntimeManagerFactory.Factory.get().newPerRequestRuntimeManager(environment);
        RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());

		ProcessInstance processInstance = runtime.getKieSession().startProcess("com.sample.bpmn.hello.fa", null);
		long workItemId = ((HumanTaskNodeInstance) ((WorkflowProcessInstance) processInstance).getNodeInstances().iterator().next()).getWorkItemId();
		long taskId = runtime.getTaskService().getTaskByWorkItemId(workItemId).getId();
		runtime.getTaskService().claim(taskId, "mary");

		List<Status> statusses = new ArrayList<Status>();
		statusses.add(Status.Reserved);

		List<TaskSummary> tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(1, tasks.size());

		taskId = tasks.get(0).getId();
		UserTransaction ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
		try {
			ut.begin();
			runtime.getTaskService().start(taskId, "mary");
			runtime.getTaskService().complete(taskId, "mary", null);
			fail("Task completed");
		} catch (RuntimeException e) {
			// do nothing
		    e.printStackTrace();
		}
		try {
		ut.rollback();
		} catch(Exception e) {
		    
		}
		// TODO: whenever transaction fails, do we need to dispose? can we?
		// sessionManager.dispose();
		manager.disposeRuntimeEngine(runtime);

		runtime = manager.getRuntimeEngine(EmptyContext.get());
		tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(1, tasks.size());
		
		manager.disposeRuntimeEngine(runtime);
	}
	
	@Test
	public void testNewSessionFailAfter2() throws Exception {
	    RuntimeEnvironment environment = RuntimeEnvironmentBuilder.Factory.get()
    			.newDefaultBuilder()
                .userGroupCallback(userGroupCallback)
                .addAsset(ResourceFactory.newClassPathResource("sampleFailAfter.bpmn"), ResourceType.BPMN2)
                .get();
        if( useLocking ) {
            environment.getEnvironment().set(EnvironmentName.USE_PESSIMISTIC_LOCKING, true); 
         }
        
        manager = RuntimeManagerFactory.Factory.get().newPerRequestRuntimeManager(environment);
        RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());

		ProcessInstance processInstance = runtime.getKieSession().startProcess("com.sample.bpmn.hello.fa", null);
		long workItemId = ((HumanTaskNodeInstance) ((WorkflowProcessInstance) processInstance).getNodeInstances().iterator().next()).getWorkItemId();
		long taskId = runtime.getTaskService().getTaskByWorkItemId(workItemId).getId();
		runtime.getTaskService().claim(taskId, "mary");
		runtime.getTaskService().start(taskId, "mary");

		List<Status> statusses = new ArrayList<Status>();
		statusses.add(Status.InProgress);

		List<TaskSummary> tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(1, tasks.size());

		taskId = tasks.get(0).getId();
		try {
		    runtime.getTaskService().complete(taskId, "mary", null);
			fail("Task completed");
		} catch (RuntimeException e) {
			// do nothing
		}

		// TODO: whenever transaction fails, do we need to dispose? can we?
		// sessionManager.dispose();
		manager.disposeRuntimeEngine(runtime);

		runtime = manager.getRuntimeEngine(EmptyContext.get());
		tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		assertEquals(1, tasks.size());
		
		manager.disposeRuntimeEngine(runtime);
	}
	
	private void testStartProcess(RuntimeEngine runtime) throws Exception {
		
		long taskId; 
		synchronized((SingleSessionCommandService) ((CommandBasedStatefulKnowledgeSession) runtime.getKieSession()).getRunner()) {
			UserTransaction ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
			ut.begin();
			logger.debug("Starting process on ksession {}", runtime.getKieSession().getIdentifier());
			ProcessInstance processInstance = runtime.getKieSession().startProcess("com.sample.bpmn.hello", null);
			logger.debug("Started process instance {} on ksession {}", processInstance.getId(), runtime.getKieSession().getIdentifier());
			long workItemId = ((HumanTaskNodeInstance) ((WorkflowProcessInstance) processInstance).getNodeInstances().iterator().next()).getWorkItemId();
			taskId = runtime.getTaskService().getTaskByWorkItemId(workItemId).getId();
			logger.debug("Created task {}", taskId);
			runtime.getTaskService().claim(taskId, "mary");
			ut.commit();
		}
		
		
	}
	
	public class StartProcessRunnable implements Runnable {
		private RuntimeManager manager;
		@SuppressWarnings("unused")
        private int counter;
		public StartProcessRunnable(RuntimeManager manager, int counter) {
			this.manager = manager;
			this.counter = counter;
		}
		public void run() {
			try {
				for (int i=0; i<nbInvocations; i++) {
				    RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
					logger.trace("Thread {} doing call {}", counter, i);
					testStartProcess(runtime);
					manager.disposeRuntimeEngine(runtime);
				}
				logger.trace("Process thread {} completed", counter);
				completedStart++;
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
	}
	
	private boolean testCompleteTask(RuntimeEngine runtime) throws InterruptedException, Exception {
		boolean result = false;
		List<Status> statusses = new ArrayList<Status>();
		statusses.add(Status.Reserved);
		
		List<TaskSummary> tasks = null;
		tasks = runtime.getTaskService().getTasksOwnedByStatus("mary", statusses, "en-UK");
		if (tasks.isEmpty()) {
		    logger.debug("Task thread found no tasks");
			Thread.sleep(1000);
		} else {
			long taskId = tasks.get(0).getId();
			logger.debug("Completing task {}", taskId);
			boolean success = false;
			try {
			    runtime.getTaskService().start(taskId, "mary");
				success = true;
			} catch (PermissionDeniedException e) {
				// TODO can we avoid these by doing it all in one transaction?
			    logger.debug("Task thread was too late for starting task {}", taskId);
			} catch (RuntimeException e) {
				if (isCausedByOptimisticLockingFailure(e)) {
				    logger.debug("Task thread got in conflict when starting task {}", taskId);
				} else {
					throw e;
				}
			}
			if (success) {
			    try {
    			    runtime.getTaskService().complete(taskId, "mary", null);
    			    logger.debug("Completed task {}", taskId);
    				result = true;
			    } catch (RuntimeException e) {
	                if (isCausedByOptimisticLockingFailure(e)) {
	                    logger.debug("Task thread got in conflict when completing task {}", taskId);
	                } else {
	                    throw e;
	                }
	            }
			}
		}
		
		return result;
	}
	
	private boolean testCompleteTaskByProcessInstance(RuntimeEngine runtime, long piId) throws InterruptedException, Exception {
        boolean result = false;
        List<Status> statusses = new ArrayList<Status>();
        statusses.add(Status.Reserved);
        
        List<TaskSummary> tasks = null;
        tasks = runtime.getTaskService().getTasksByStatusByProcessInstanceId(piId, statusses, "en-UK");
        if (tasks.isEmpty()) {
            logger.debug("Task thread found no tasks");
            Thread.sleep(1000);
        } else {
            long taskId = tasks.get(0).getId();
            logger.debug("Completing task {}", taskId);
            boolean success = false;
            try {
                runtime.getTaskService().start(taskId, "mary");
                success = true;
            } catch (PermissionDeniedException e) {
                // TODO can we avoid these by doing it all in one transaction?
                logger.debug("Task thread was too late for starting task {}", taskId);
            } catch (RuntimeException e) {
                if (isCausedByOptimisticLockingFailure(e)) {
                    logger.debug("Task thread got in conflict when starting task {}", taskId);
                } else {
                    throw e;
                }
            }
            if (success) {
                runtime.getTaskService().complete(taskId, "mary", null);
                logger.debug("Completed task {}", taskId);
                result = true;
   
            }
        }
        
        return result;
    }

	public class CompleteTaskRunnable implements Runnable {
		private RuntimeManager manager;
		@SuppressWarnings("unused")
        private int counter;
		public CompleteTaskRunnable(RuntimeManager manager, int counter) {
			this.manager = manager;
			this.counter = counter;
		}
		public void run() {
			try {
				int i = 0;
				while (i < nbInvocations) {
				    RuntimeEngine runtime = manager.getRuntimeEngine(EmptyContext.get());
					boolean success = testCompleteTask(runtime);
					int retry = 10;
					while (retry > 0) {
						try {
							manager.disposeRuntimeEngine(runtime);
							retry = 0;
						} catch (Throwable t) {
							// This can happen so we need to re-attempt the dispose
//						java.lang.RuntimeException: java.lang.IllegalStateException: java.lang.reflect.InvocationTargetException
//						at org.jbpm.runtime.manager.impl.PerRequestRuntimeManager.disposeRuntimeEngine(PerRequestRuntimeManager.java:156)
//						at org.jbpm.runtime.manager.concurrent.SessionTest$CompleteTaskRunnable.run(SessionTest.java:692)
//						at java.lang.Thread.run(Thread.java:745)
//						Caused by: java.lang.IllegalStateException: java.lang.reflect.InvocationTargetException
//						at org.drools.persistence.jpa.KnowledgeStoreServiceImpl.buildCommandService(KnowledgeStoreServiceImpl.java:164)
//						at org.drools.persistence.jpa.KnowledgeStoreServiceImpl.newKieSession(KnowledgeStoreServiceImpl.java:67)
//						at org.drools.persistence.jpa.KnowledgeStoreServiceImpl.newKieSession(KnowledgeStoreServiceImpl.java:36)
//						at org.kie.internal.persistence.jpa.JPAKnowledgeService.newStatefulKnowledgeSession(JPAKnowledgeService.java:121)
//						at org.jbpm.runtime.manager.impl.factory.JPASessionFactory.newKieSession(JPASessionFactory.java:43)
//						at org.jbpm.runtime.manager.impl.PerRequestRuntimeManager$PerRequestInitializer.initKieSession(PerRequestRuntimeManager.java:212)
//						at org.jbpm.runtime.manager.impl.RuntimeEngineImpl.getKieSession(RuntimeEngineImpl.java:72)
//						at org.jbpm.runtime.manager.impl.AbstractRuntimeManager.canDispose(AbstractRuntimeManager.java:195)
//						at org.jbpm.runtime.manager.impl.PerRequestRuntimeManager.disposeRuntimeEngine(PerRequestRuntimeManager.java:135)
//	... 2 more
//						Caused by: java.lang.reflect.InvocationTargetException
//						at sun.reflect.GeneratedConstructorAccessor58.newInstance(Unknown Source)
//						at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
//						at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
//						at org.drools.persistence.jpa.KnowledgeStoreServiceImpl.buildCommandService(KnowledgeStoreServiceImpl.java:150)
//	... 10 more
//						Caused by: java.lang.RuntimeException: Unable to commit transaction
//						at org.drools.persistence.jta.JtaTransactionManager.commit(JtaTransactionManager.java:248)
//						at org.drools.persistence.PersistableRunner.<init>(PersistableRunner.java:117)
//	... 14 more
//						Caused by: javax.transaction.RollbackException: ARJUNA016053: Could not commit transaction.
//						at com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple.commitAndDisassociate(TransactionImple.java:1301)
//						at com.arjuna.ats.internal.jta.transaction.arjunacore.BaseTransaction.commit(BaseTransaction.java:126)
//						at org.drools.persistence.jta.JtaTransactionManager.commit(JtaTransactionManager.java:245)
//	... 15 more
//						Caused by: javax.persistence.OptimisticLockException: Batch update returned unexpected row count from update [0]; actual row count: 0; expected: 1
//						at org.hibernate.jpa.spi.AbstractEntityManagerImpl.wrapStaleStateException(AbstractEntityManagerImpl.java:1729)
//						at org.hibernate.jpa.spi.AbstractEntityManagerImpl.convert(AbstractEntityManagerImpl.java:1634)
//						at org.hibernate.jpa.spi.AbstractEntityManagerImpl.convert(AbstractEntityManagerImpl.java:1602)
//						at org.hibernate.jpa.spi.AbstractEntityManagerImpl.convert(AbstractEntityManagerImpl.java:1608)
//						at org.hibernate.jpa.spi.AbstractEntityManagerImpl.flush(AbstractEntityManagerImpl.java:1303)
//						at org.drools.persistence.TriggerUpdateTransactionSynchronization.beforeCompletion(TriggerUpdateTransactionSynchronization.java:76)
//						at org.drools.persistence.jta.JtaTransactionSynchronizationAdapter.beforeCompletion(JtaTransactionSynchronizationAdapter.java:54)
//						at com.arjuna.ats.internal.jta.resources.arjunacore.SynchronizationImple.beforeCompletion(SynchronizationImple.java:76)
//						at com.arjuna.ats.arjuna.coordinator.TwoPhaseCoordinator.beforeCompletion(TwoPhaseCoordinator.java:368)
//						at com.arjuna.ats.arjuna.coordinator.TwoPhaseCoordinator.end(TwoPhaseCoordinator.java:91)
//						at com.arjuna.ats.arjuna.AtomicAction.commit(AtomicAction.java:162)
//						at com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple.commitAndDisassociate(TransactionImple.java:1289)
//	... 17 more
//						Caused by: org.hibernate.StaleStateException: Batch update returned unexpected row count from update [0]; actual row count: 0; expected: 1
//						at org.hibernate.jdbc.Expectations$BasicExpectation.checkBatched(Expectations.java:67)
//						at org.hibernate.jdbc.Expectations$BasicExpectation.verifyOutcome(Expectations.java:54)
//						at org.hibernate.engine.jdbc.batch.internal.NonBatchingBatch.addToBatch(NonBatchingBatch.java:46)
//						at org.hibernate.persister.entity.AbstractEntityPersister.update(AbstractEntityPersister.java:3184)
//						at org.hibernate.persister.entity.AbstractEntityPersister.updateOrInsert(AbstractEntityPersister.java:3063)
//						at org.hibernate.persister.entity.AbstractEntityPersister.update(AbstractEntityPersister.java:3443)
//						at org.hibernate.action.internal.EntityUpdateAction.execute(EntityUpdateAction.java:145)
//						at org.hibernate.engine.spi.ActionQueue.executeActions(ActionQueue.java:586)
//						at org.hibernate.engine.spi.ActionQueue.executeActions(ActionQueue.java:460)
//						at org.hibernate.event.internal.AbstractFlushingEventListener.performExecutions(AbstractFlushingEventListener.java:337)
//						at org.hibernate.event.internal.DefaultFlushEventListener.onFlush(DefaultFlushEventListener.java:39)
//						at org.hibernate.internal.SessionImpl.flush(SessionImpl.java:1295)
//						at org.hibernate.jpa.spi.AbstractEntityManagerImpl.flush(AbstractEntityManagerImpl.java:1300)
//	... 24 more
							t.printStackTrace();
							retry--;
						}
					}
					if (success) {
						i++;
					}
				}
				completedTask++;
				logger.trace("Task thread {} completed", counter);
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
	}
	
    public class StartProcessPerProcessInstanceRunnable implements Runnable {
        private RuntimeManager manager;
        @SuppressWarnings("unused")
        private int counter;
        public StartProcessPerProcessInstanceRunnable(RuntimeManager manager, int counter) {
            this.manager = manager;
            this.counter = counter;
        }
        public void run() {
            try {
                for (int i=0; i<nbInvocations; i++) {
                    RuntimeEngine runtime = manager.getRuntimeEngine(ProcessInstanceIdContext.get());
                  logger.trace("Thread {} doing call {}", counter, i);
                    testStartProcess(runtime);
                    manager.disposeRuntimeEngine(runtime);
                }
              logger.trace("Process thread {} completed", counter);
                completedStart++;
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
	
   public class CompleteTaskPerProcessInstanceRunnable implements Runnable {
        private RuntimeManager manager;
        private int counter;
        public CompleteTaskPerProcessInstanceRunnable(RuntimeManager manager, int counter) {
            this.manager = manager;
            this.counter = counter;
        }
        public void run() {
            try {
                int i = 0;
                while (i < nbInvocations) {

                    long processInstanceId = (nbInvocations *counter)+1 + i;
                    logger.trace("pi id {} counter {}", processInstanceId, counter);
                    RuntimeEngine runtime = manager.getRuntimeEngine(ProcessInstanceIdContext.get(processInstanceId));
                    boolean success = false;
                    
                    success = testCompleteTaskByProcessInstance(runtime, processInstanceId);
                    
                    manager.disposeRuntimeEngine(runtime);
                    if (success) {
                        i++;
                    }
                }
                completedTask++;
	              logger.trace("Task thread {} completed", counter);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
   
   protected boolean isCausedByOptimisticLockingFailure(Throwable throwable) {


       while (throwable != null) {
           if (OptimisticLockException.class.isAssignableFrom(throwable.getClass())
        		   || StaleObjectStateException.class.isAssignableFrom(throwable.getClass())) {
               return true;
           } else {
               throwable = throwable.getCause();
           }
       }

       return false;
   }
}
