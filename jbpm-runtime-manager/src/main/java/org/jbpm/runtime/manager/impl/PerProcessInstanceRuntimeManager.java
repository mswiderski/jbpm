/*
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jbpm.runtime.manager.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.drools.core.command.CommandService;
import org.drools.core.command.SingleSessionCommandService;
import org.drools.core.command.impl.CommandBasedStatefulKnowledgeSession;
import org.drools.core.command.impl.GenericCommand;
import org.drools.core.command.impl.KnowledgeCommandContext;
import org.drools.core.common.InternalKnowledgeRuntime;
import org.drools.persistence.OrderedTransactionSynchronization;
import org.drools.persistence.TransactionManager;
import org.drools.persistence.TransactionManagerHelper;
import org.jbpm.runtime.manager.impl.factory.LocalTaskServiceFactory;
import org.jbpm.runtime.manager.impl.mapper.EnvironmentAwareProcessInstanceContext;
import org.jbpm.runtime.manager.impl.mapper.InMemoryMapper;
import org.jbpm.runtime.manager.impl.mapper.InternalMapper;
import org.jbpm.runtime.manager.impl.mapper.JPAMapper;
import org.jbpm.runtime.manager.impl.tx.DestroySessionTransactionSynchronization;
import org.jbpm.runtime.manager.impl.tx.DisposeSessionTransactionSynchronization;
import org.jbpm.services.task.impl.TaskContentRegistry;
import org.kie.api.event.process.DefaultProcessEventListener;
import org.kie.api.event.process.ProcessCompletedEvent;
import org.kie.api.event.process.ProcessStartedEvent;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.manager.Context;
import org.kie.api.runtime.manager.RuntimeEngine;
import org.kie.api.runtime.manager.RuntimeEnvironment;
import org.kie.api.task.TaskService;
import org.kie.internal.runtime.manager.Disposable;
import org.kie.internal.runtime.manager.InternalRuntimeManager;
import org.kie.internal.runtime.manager.Mapper;
import org.kie.internal.runtime.manager.SessionFactory;
import org.kie.internal.runtime.manager.SessionNotFoundException;
import org.kie.internal.runtime.manager.TaskServiceFactory;
import org.kie.internal.runtime.manager.context.CorrelationKeyContext;
import org.kie.internal.runtime.manager.context.EmptyContext;
import org.kie.internal.runtime.manager.context.ProcessInstanceIdContext;
import org.kie.internal.task.api.ContentMarshallerContext;
import org.kie.internal.task.api.InternalTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RuntimeManager implementation that is backed by the "Per Process Instance" strategy. This means that every
 * process instance will be bound to a ksession for it's entire life time.  Once started, whenever other operations are invoked,
 * this manager will ensure that the correct ksession will be provided.
 * <br/>
 * This also applies to sub processes (reusable sub processes) that create new process instances: the sub process instance
 * will have its own ksession independent of the parent one.
 * <br/>
 * This manager will ensure that as soon as the process instance completes, the ksession will be disposed of and destroyed.
 * <br/>
 * This implementation supports the following <code>Context</code> implementations:
 * <ul>
 *  <li>ProcessInstanceIdContext</li>
 *  <li>CorrelationKeyContext</li>
 *  <li>EmptyContext - for initial RuntimeEngine to start process only</li>
 * </ul>
 */
public class PerProcessInstanceRuntimeManager extends AbstractRuntimeManager {

	private static final Logger logger = LoggerFactory.getLogger(PerProcessInstanceRuntimeManager.class);

	private boolean useLocking = Boolean.parseBoolean(System.getProperty("org.jbpm.runtime.manager.ppi.lock", "true"));

    private SessionFactory factory;
    private TaskServiceFactory taskServiceFactory;

    private static ThreadLocal<Map<Object, RuntimeEngine>> local = new ThreadLocal<Map<Object, RuntimeEngine>>();

    private ConcurrentMap<Long, ReentrantLock> engineLocks = new ConcurrentHashMap<Long, ReentrantLock>();

    private Mapper mapper;

    public PerProcessInstanceRuntimeManager(RuntimeEnvironment environment, SessionFactory factory, TaskServiceFactory taskServiceFactory, String identifier) {
        super(environment, identifier);
        this.factory = factory;
        this.taskServiceFactory = taskServiceFactory;
        this.mapper = ((org.kie.internal.runtime.manager.RuntimeEnvironment)environment).getMapper();
        this.registry.register(this);
    }

    @Override
    public RuntimeEngine getRuntimeEngine(Context<?> context) {
    	if (isClosed()) {
    		throw new IllegalStateException("Runtime manager " + identifier + " is already closed");
    	}
    	checkPermission();
    	RuntimeEngine runtime = null;
    	Object contextId = context.getContextId();

    	if (!(context instanceof ProcessInstanceIdContext || context instanceof CorrelationKeyContext)) {
    		logger.warn("ProcessInstanceIdContext or CorrelationKeyContext shall be used when interacting with PerProcessInstance runtime manager");
    	}

    	if (engineInitEager) {
			KieSession ksession = null;
			Long ksessionId = null;
			if (contextId == null || context instanceof EmptyContext) {
				ksession = factory.newKieSession();
				ksessionId = ksession.getIdentifier();
			} else {
				RuntimeEngine localRuntime = findLocalRuntime(contextId);
				if (localRuntime != null) {
					return localRuntime;
				}
				ksessionId = mapper.findMapping(context, this.identifier);
				if (ksessionId == null) {
					throw new SessionNotFoundException("No session found for context " + context.getContextId());
				}
				ksession = factory.findKieSessionById(ksessionId);
			}
			InternalTaskService internalTaskService = (InternalTaskService) taskServiceFactory.newTaskService();
			runtime = new RuntimeEngineImpl(ksession, internalTaskService);
			((RuntimeEngineImpl) runtime).setManager(this);
			((RuntimeEngineImpl) runtime).setContext(context);
			configureRuntimeOnTaskService(internalTaskService, runtime);
			registerDisposeCallback(runtime, new DisposeSessionTransactionSynchronization(this, runtime));
			registerItems(runtime);
			attachManager(runtime);
			ksession.addEventListener(new MaintainMappingListener(ksessionId, runtime, this.identifier));
    	} else {
    		RuntimeEngine localRuntime = findLocalRuntime(contextId);
			if (localRuntime != null) {
				return localRuntime;
			}
    		// lazy initialization of ksession and task service

	    	runtime = new RuntimeEngineImpl(context, new PerProcessInstanceInitializer());
	        ((RuntimeEngineImpl) runtime).setManager(this);

    	}
    	createLockOnGetEngine(context, runtime);
        saveLocalRuntime(contextId, runtime);

        return runtime;
    }

    @Override
    public void signalEvent(String type, Object event) {

        // first signal with new context in case there are start event with signal
        RuntimeEngine runtimeEngine = getRuntimeEngine(ProcessInstanceIdContext.get());
        runtimeEngine.getKieSession().signalEvent(type, event);

        disposeRuntimeEngine(runtimeEngine);

        // next find out all instances waiting for given event type
        List<String> processInstances = ((InternalMapper) mapper).findContextIdForEvent(type, getIdentifier());
        for (String piId : processInstances) {
            runtimeEngine = getRuntimeEngine(ProcessInstanceIdContext.get(Long.parseLong(piId)));
            runtimeEngine.getKieSession().signalEvent(type, event);

            disposeRuntimeEngine(runtimeEngine);

        }

        // process currently active runtime engines
        Map<Object, RuntimeEngine> currentlyActive = local.get();
        if (currentlyActive != null && !currentlyActive.isEmpty()) {
            RuntimeEngine[] activeEngines = currentlyActive.values().toArray(new RuntimeEngine[currentlyActive.size()]);
            for (RuntimeEngine engine : activeEngines) {
                Context<?> context = ((RuntimeEngineImpl) engine).getContext();
                if (context != null && context instanceof ProcessInstanceIdContext
                        && ((ProcessInstanceIdContext) context).getContextId() != null) {
                    engine.getKieSession().signalEvent(type, event, ((ProcessInstanceIdContext) context).getContextId());
                }
            }
        }
    }


    @Override
    public void validate(KieSession ksession, Context<?> context) throws IllegalStateException {
    	if (isClosed()) {
    		throw new IllegalStateException("Runtime manager " + identifier + " is already closed");
    	}
        if (context == null || context.getContextId() == null) {
            return;
        }
        Long ksessionId = mapper.findMapping(context, this.identifier);

        if (ksessionId == null) {
            // make sure ksession is not use by any other context
            Object contextId = mapper.findContextId(ksession.getIdentifier(), this.identifier);
            if (contextId != null) {
                throw new IllegalStateException("KieSession with id " + ksession.getIdentifier() + " is already used by another context");
            }
            return;
        }
        if (ksession.getIdentifier() != ksessionId) {
            throw new IllegalStateException("Invalid session was used for this context " + context);
        }

    }

    @Override
    public void disposeRuntimeEngine(RuntimeEngine runtime) {
    	if (isClosed()) {
    		throw new IllegalStateException("Runtime manager " + identifier + " is already closed");
    	}

    	try {
        	if (canDispose(runtime)) {
            	removeLocalRuntime(runtime);
            	if (runtime instanceof Disposable) {
                	// special handling for in memory to not allow to dispose if there is any context in the mapper
                	if (mapper instanceof InMemoryMapper && ((InMemoryMapper)mapper).hasContext(runtime.getKieSession().getIdentifier())){
                		return;
                	}
                    ((Disposable) runtime).dispose();
                }

            	releaseAndCleanLock(runtime);
        	}
    	} catch (Exception e) {
    	    releaseAndCleanLock(runtime);
    	    throw new RuntimeException(e);
    	}
    }


    @Override
    public void softDispose(RuntimeEngine runtimeEngine) {
        super.softDispose(runtimeEngine);
        removeLocalRuntime(runtimeEngine);

    }

    @Override
    public void close() {
        try {
        	if (!(taskServiceFactory instanceof LocalTaskServiceFactory)) {
                // if it's CDI based (meaning single application scoped bean) we need to unregister context
                removeRuntimeFromTaskService();
            }
        } catch(Exception e) {
           // do nothing
        }
        super.close();
        factory.close();
    }


    public boolean validate(Long ksessionId, Long processInstanceId) {
    	Long mapped = this.mapper.findMapping(ProcessInstanceIdContext.get(processInstanceId), this.identifier);
        if (mapped == ksessionId) {
            return true;
        }

        return false;
    }


    private class MaintainMappingListener extends DefaultProcessEventListener {

        private Long ksessionId;
        private RuntimeEngine runtime;
        private String managerId;

        MaintainMappingListener(Long ksessionId, RuntimeEngine runtime, String managerId) {
        	this.ksessionId = ksessionId;
            this.runtime = runtime;
            this.managerId = managerId;
        }
        @Override
        public void afterProcessCompleted(ProcessCompletedEvent event) {
            mapper.removeMapping(new EnvironmentAwareProcessInstanceContext(
            		event.getKieRuntime().getEnvironment(),
            		event.getProcessInstance().getId()), managerId);
            removeLocalRuntime(runtime);

            factory.onDispose(runtime.getKieSession().getIdentifier());
            registerDisposeCallback(runtime,
                        new DestroySessionTransactionSynchronization(runtime.getKieSession()));
        }

        @Override
        public void beforeProcessStarted(ProcessStartedEvent event) {
            mapper.saveMapping(new EnvironmentAwareProcessInstanceContext(
            		event.getKieRuntime().getEnvironment(),
            		event.getProcessInstance().getId()), ksessionId, managerId);
            saveLocalRuntime(event.getProcessInstance().getId(), runtime);
            ((RuntimeEngineImpl)runtime).setContext(ProcessInstanceIdContext.get(event.getProcessInstance().getId()));

            createLockOnNewProcessInstance(event.getProcessInstance().getId(), runtime);
        }

    }


    public SessionFactory getFactory() {
        return factory;
    }

    public void setFactory(SessionFactory factory) {
        this.factory = factory;
    }

    public TaskServiceFactory getTaskServiceFactory() {
        return taskServiceFactory;
    }

    public void setTaskServiceFactory(TaskServiceFactory taskServiceFactory) {
        this.taskServiceFactory = taskServiceFactory;
    }

    public Mapper getMapper() {
        return mapper;
    }

    public void setMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    protected RuntimeEngine findLocalRuntime(Object processInstanceId) {
        if (processInstanceId == null) {
            return null;
        }
        Map<Object, RuntimeEngine> map = local.get();
        if (map == null) {
            return null;
        } else {
        	RuntimeEngine engine = map.get(processInstanceId);
        	// check if engine is not already disposed as afterCompletion might be issued from another thread
        	if (engine != null && ((RuntimeEngineImpl) engine).isDisposed()) {
        		map.remove(processInstanceId);
        		return null;
        	}

        	return engine;
        }
    }

    protected void saveLocalRuntime(Object processInstanceId, RuntimeEngine runtime) {
        // since this manager is strictly for process instance ids it should only store
        // process instance ids as local cache keys
        if (processInstanceId == null || !(processInstanceId instanceof Long)) {
            return;
        }
        Map<Object, RuntimeEngine> map = local.get();
        if (map == null) {
            map = new HashMap<Object, RuntimeEngine>();
            local.set(map);
        }

        map.put(processInstanceId, runtime);

    }

    protected void removeLocalRuntime(RuntimeEngine runtime) {
        Map<Object, RuntimeEngine> map = local.get();
        Object keyToRemove = -1l;
        if (map != null) {
            for (Map.Entry<Object, RuntimeEngine> entry : map.entrySet()) {
                if (runtime.equals(entry.getValue())) {
                    keyToRemove = entry.getKey();
                    break;
                }
            }

            map.remove(keyToRemove);
        }
    }

    @Override
    public void init() {

    	TaskContentRegistry.get().addMarshallerContext(getIdentifier(),
    			new ContentMarshallerContext(environment.getEnvironment(), environment.getClassLoader()));

    	boolean owner = false;
    	TransactionManager tm = null;
    	if (environment.usePersistence()){
        	tm = getTransactionManagerInternal(environment.getEnvironment());
        	owner = tm.begin();
    	}
        try {
        	// need to init one session to bootstrap all case - such as start timers
            KieSession initialKsession = factory.newKieSession();
            // there is a need to call getProcessRuntime otherwise the start listeners are not registered
            initialKsession.execute(new GenericCommand<Void>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Void execute(org.kie.internal.command.Context context) {
                    KieSession ksession = ((KnowledgeCommandContext) context).getKieSession();
                    ((InternalKnowledgeRuntime) ksession).getProcessRuntime();
                    return null;
                }
            });
            factory.onDispose(initialKsession.getIdentifier());
            initialKsession.execute(new DestroyKSessionCommand(initialKsession, this));

            if (!"false".equalsIgnoreCase(System.getProperty("org.jbpm.rm.init.timer"))) {
            	if (mapper instanceof JPAMapper) {
            		List<Long> ksessionsToInit = ((JPAMapper) mapper).findKSessionToInit(this.identifier);
            		for (Long id : ksessionsToInit) {
            			initialKsession = factory.findKieSessionById(id);
            			initialKsession.execute(new DisposeKSessionCommand(initialKsession, this));
            		}
            	}
            }
            if (tm != null) {
                tm.commit(owner);
            }
        } catch (Exception e) {
            if (tm != null) {
                tm.rollback(owner);
            }
            throw new RuntimeException("Exception while initializing runtime manager " + this.identifier, e);
        }
    }


    private static class DestroyKSessionCommand implements GenericCommand<Void> {
        private static final long serialVersionUID = 1L;

        private KieSession initialKsession;
        private AbstractRuntimeManager manager;

        public DestroyKSessionCommand(KieSession initialKsession, AbstractRuntimeManager manager) {
        	this.initialKsession = initialKsession;
        	this.manager = manager;
        }

        @Override
        public Void execute(org.kie.internal.command.Context context) {
        	TransactionManager tm = (TransactionManager) initialKsession.getEnvironment().get(EnvironmentName.TRANSACTION_MANAGER);
            if (manager.hasEnvironmentEntry("IS_JTA_TRANSACTION", false)) {
            	if (initialKsession instanceof CommandBasedStatefulKnowledgeSession) {
                    CommandService commandService = ((CommandBasedStatefulKnowledgeSession) initialKsession).getCommandService();
                    ((SingleSessionCommandService) commandService).destroy();
                 } else {
            		((KnowledgeCommandContext) context).getKieSession().destroy();
            	}
            	return null;
        	}

            if (tm != null && tm.getStatus() != TransactionManager.STATUS_NO_TRANSACTION
                    && tm.getStatus() != TransactionManager.STATUS_ROLLEDBACK
                    && tm.getStatus() != TransactionManager.STATUS_COMMITTED) {
            	TransactionManagerHelper.registerTransactionSyncInContainer(tm, new OrderedTransactionSynchronization(5, "PPIRM-"+initialKsession.getIdentifier()) {

                    @Override
                    public void beforeCompletion() {
                        if (initialKsession instanceof CommandBasedStatefulKnowledgeSession) {
                            CommandService commandService = ((CommandBasedStatefulKnowledgeSession) initialKsession).getCommandService();
                            ((SingleSessionCommandService) commandService).destroy();
                         }
                    }

                    @Override
                    public void afterCompletion(int arg0) {
                    	initialKsession.dispose();

                    }
				});
            } else {
            	initialKsession.destroy();
            }
            return null;
        }
    }

    private static class DisposeKSessionCommand implements GenericCommand<Void> {
        private static final long serialVersionUID = 1L;

        private KieSession initialKsession;
        private AbstractRuntimeManager manager;

        public DisposeKSessionCommand(KieSession initialKsession, AbstractRuntimeManager manager) {
        	this.initialKsession = initialKsession;
        	this.manager = manager;
        }

        @Override
        public Void execute(org.kie.internal.command.Context context) {

            if (manager.hasEnvironmentEntry("IS_JTA_TRANSACTION", false)) {
            	initialKsession.dispose();
            	return null;
        	}
            TransactionManager tm = (TransactionManager) initialKsession.getEnvironment().get(EnvironmentName.TRANSACTION_MANAGER);
            if (tm != null && tm.getStatus() != TransactionManager.STATUS_NO_TRANSACTION
                    && tm.getStatus() != TransactionManager.STATUS_ROLLEDBACK
                    && tm.getStatus() != TransactionManager.STATUS_COMMITTED) {
            	TransactionManagerHelper.registerTransactionSyncInContainer(tm, new OrderedTransactionSynchronization(5, "PPIRM-"+initialKsession.getIdentifier()) {

                    @Override
                    public void beforeCompletion() {
                    }

                    @Override
                    public void afterCompletion(int arg0) {
                    	initialKsession.dispose();

                    }
				});
            } else {
            	initialKsession.dispose();
            }
            return null;
        }
    }

    private class PerProcessInstanceInitializer implements RuntimeEngineInitlializer {


    	@Override
    	public KieSession initKieSession(Context<?> context, InternalRuntimeManager manager, RuntimeEngine engine) {


    		Object contextId = context.getContextId();

    		if (contextId != null && !(context instanceof EmptyContext)) {
	    		Long found = mapper.findMapping(context, manager.getIdentifier());
			    if (found == null) {
			    	removeLocalRuntime(engine);
			        throw new SessionNotFoundException("No session found for context " + context.getContextId());
			    }
	    	}

    		KieSession ksession = null;
    		Long ksessionId = null;
            if (contextId == null || context instanceof EmptyContext ) {
                ksession = factory.newKieSession();
                ksessionId = ksession.getIdentifier();
            } else {
                RuntimeEngine localRuntime = ((PerProcessInstanceRuntimeManager)manager).findLocalRuntime(contextId);
                if (localRuntime != null && ((RuntimeEngineImpl)engine).internalGetKieSession() != null) {
                    return localRuntime.getKieSession();
                }
                ksessionId = mapper.findMapping(context, manager.getIdentifier());
                if (ksessionId == null) {
                    throw new SessionNotFoundException("No session found for context " + context.getContextId());
                }
                ksession = factory.findKieSessionById(ksessionId);
            }
            ((RuntimeEngineImpl)engine).internalSetKieSession(ksession);
            registerItems(engine);
            attachManager(engine);
            registerDisposeCallback(engine, new DisposeSessionTransactionSynchronization(manager, engine));
            ksession.addEventListener(new MaintainMappingListener(ksessionId, engine, manager.getIdentifier()));
    		return ksession;
    	}

    	@Override
    	public TaskService initTaskService(Context<?> context, InternalRuntimeManager manager, RuntimeEngine engine) {
    		InternalTaskService internalTaskService = (InternalTaskService) taskServiceFactory.newTaskService();
    		registerDisposeCallback(engine, new DisposeSessionTransactionSynchronization(manager, engine));
            configureRuntimeOnTaskService(internalTaskService, engine);
    		return internalTaskService;
    	}

    }


    /*
     * locking support for same context - runtime engine that deals with exact same process instance context
     */

    protected void createLockOnGetEngine(Context<?> context, RuntimeEngine runtime) {
        if (!useLocking) {
            logger.debug("Locking on runtime manager disabled");
            return;
        }

        if (context instanceof ProcessInstanceIdContext) {
            Long piId = ((ProcessInstanceIdContext) context).getContextId();
            if (piId != null) {
                ReentrantLock newLock = new ReentrantLock();
                ReentrantLock lock = engineLocks.putIfAbsent(piId, newLock);
                if (lock == null) {
                    lock = newLock;
                }
                logger.debug("Trying to get a lock {} for {} by {}", lock, context, runtime);
                lock.lock();
                logger.debug("Lock {} taken for {} by {} for waiting threads by {}", lock, context, runtime, lock.hasQueuedThreads());

            }
        }
    }

    protected void createLockOnNewProcessInstance(Long processInstanceId, RuntimeEngine runtime) {
        if (!useLocking) {
            logger.debug("Locking on runtime manager disabled");
            return;
        }
        ReentrantLock newLock = new ReentrantLock();
        ReentrantLock lock = engineLocks.putIfAbsent(processInstanceId, newLock);
        if (lock == null) {
            lock = newLock;
        }
        lock.lock();
        logger.debug("[MaintainMappingListener] Lock {} created and stored in list by {}", lock, runtime);
    }


    protected void releaseAndCleanLock(RuntimeEngine runtime) {
        if (!useLocking) {
            logger.debug("Locking on runtime manager disabled");
            return;
        }
        if (((RuntimeEngineImpl)runtime).getContext() instanceof ProcessInstanceIdContext) {
            Long piId = ((ProcessInstanceIdContext) ((RuntimeEngineImpl)runtime).getContext()).getContextId();
            if (piId != null) {
                ReentrantLock lock = engineLocks.get(piId);

                if (!lock.hasQueuedThreads()) {
                    logger.debug("Removing lock {} from list as non is waiting for it by {}", lock, runtime);
                    engineLocks.remove(piId);
                }
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                    logger.debug("{} unlocked by {}", lock, runtime);
                }
            }
        }
    }
}
