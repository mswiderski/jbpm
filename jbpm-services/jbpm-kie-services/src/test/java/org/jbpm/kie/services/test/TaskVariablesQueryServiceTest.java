/*
 * Copyright 2014 JBoss by Red Hat.
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

package org.jbpm.kie.services.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.kie.scanner.MavenRepository.getMavenRepository;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.jbpm.kie.services.impl.KModuleDeploymentUnit;
import org.jbpm.kie.test.util.AbstractKieServicesBaseTest;
import org.jbpm.services.api.model.DeployedUnit;
import org.jbpm.services.api.model.DeploymentUnit;
import org.jbpm.services.api.model.ProcessDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.builder.ReleaseId;
import org.kie.api.runtime.query.QueryContext;
import org.kie.api.task.model.Status;
import org.kie.api.task.model.TaskSummary;
import org.kie.internal.query.QueryFilter;
import org.kie.scanner.MavenRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This test should not be seen as part of the regular test suite... at least not for now
 * it is for demo purpose of task variables search capabilities
 *
 */
public class TaskVariablesQueryServiceTest extends AbstractKieServicesBaseTest {
    
    private static final Logger logger = LoggerFactory.getLogger(TaskVariablesQueryServiceTest.class);
   
    private List<DeploymentUnit> units = new ArrayList<DeploymentUnit>();
    
    private static final String ARTIFACT_ID = "task-vars";
    private static final String GROUP_ID = "org.jbpm.test";
    private static final String VERSION = "1.0";
    
    private static List<String> productCodes = new ArrayList<String>();
    private static List<String> countries = new ArrayList<String>();
    private static List<Integer> zipCodes = new ArrayList<Integer>();
    
    private String deploymentUnitId;
    
    @BeforeClass
    public static void fillTestData() {
        Random random = new Random();
        
        productCodes.add("BPMS");
        productCodes.add("BRMS");
        productCodes.add("KIE");
        productCodes.add("Fuse");
        productCodes.add("EAP");
        productCodes.add("WILDFLY");
        productCodes.add("TOMCAT");
        productCodes.add("APACHE");
        productCodes.add("WEBSPHERE");
        productCodes.add("WEBLOGIC");
        
        countries.add("United States");
        countries.add("United Kindgdom");
        countries.add("Belgium");
        countries.add("Poland");
        countries.add("Brazil");
        countries.add("Australia");
        countries.add("Netherland");
        countries.add("Italy");
        countries.add("Canada");
        countries.add("Finland");
        
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        zipCodes.add(random.nextInt(1000));
        
        
        System.setProperty("org.jbpm.ht.callback", "custom");
        System.setProperty("org.jbpm.ht.custom.callback", "org.jbpm.services.task.identity.AcceptAllMvelUserGroupCallbackImpl");
    }
    
    @Before
    public void prepare() {
    	configureServices();
        KieServices ks = KieServices.Factory.get();
        ReleaseId releaseId = ks.newReleaseId(GROUP_ID, ARTIFACT_ID, VERSION);
        File kjar = new File("src/test/resources/kjar-task-vars/task-vars-1.0.jar");
        File pom = new File("src/test/resources/kjar-task-vars/pom.xml");
        MavenRepository repository = getMavenRepository();
        repository.deployArtifact(releaseId, kjar, pom);
        
        assertNotNull(deploymentService);
        
        KModuleDeploymentUnit deploymentUnit = new KModuleDeploymentUnit(GROUP_ID, ARTIFACT_ID, VERSION);

        deploymentService.deploy(deploymentUnit);

        DeployedUnit deployed = deploymentService.getDeployedUnit(deploymentUnit.getIdentifier());
        assertNotNull(deployed);
        assertNotNull(deployed.getDeploymentUnit());

        assertNotNull(runtimeDataService);
        Collection<ProcessDefinition> processes = runtimeDataService.getProcesses(new QueryContext());
        assertNotNull(processes);
        assertEquals(1, processes.size());
        
        deploymentUnitId = deploymentUnit.getIdentifier();
    }
    
    @After
    public void cleanup() {
        cleanupSingletonSessionId();
        if (units != null && !units.isEmpty()) {
            for (DeploymentUnit unit : units) {
                deploymentService.undeploy(unit);
            }
            units.clear();
        }
        close();
    }
    
    @Test
    public void testSearchForTasksByVariables() throws Exception {
        
        
        
        List<Status> status = new ArrayList<Status>();
        status.add(Status.Reserved);
        
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("ProductCode", "Product-XXX");
        params.put("Country", "XXX");
        params.put("ZipCode", -1);
        params.put("Delivery", false);
        
        Long processInstanceId = processService.startProcess(deploymentUnitId, "task-vars.TaskWithVars", params);
        
        List<TaskSummary> tasks = runtimeDataService.getTasksByStatusByProcessInstanceId(processInstanceId, status, new QueryFilter());
        
        assertEquals(1, tasks.size());
        
        Long taskId = tasks.get(0).getId();
        
        // now search by single task variable and its expected value        
        List<TaskSummary> myTasks = runtimeDataService.taskSummaryQuery("john")
        .variableName("productCode")
        .and()
        .variableValue("Product-XXX")
        .build()
        .getResultList();
        
        assertEquals(1, myTasks.size());        
        assertEquals(taskId, myTasks.get(0).getId());
               
        // now let's complete the task with updates to all variables
        userTaskService.start(taskId, "john");
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("productCodeOut", "Product-123");
        data.put("countryOut", "Poland");
        data.put("zipCodeOut", 1234);
        data.put("deliveryOut", true);
        userTaskService.complete(taskId, "john", data);
        
        
        // let's fine new task that waits for mary
        tasks = runtimeDataService.getTasksByStatusByProcessInstanceId(processInstanceId, status, new QueryFilter());
        
        assertEquals(1, tasks.size());        
        taskId = tasks.get(0).getId();
        
        // search by outdated task variable value that was before john updated it
        myTasks = runtimeDataService.taskSummaryQuery("mary")
                .variableName("productCode")
                .and()
                .variableValue("Product-XXX")
                .build()
                .getResultList();
        // expected no results                
        assertEquals(0, myTasks.size());
        // now let's search by two task variables where one of the values is incorrect (1233 is wrong zipCode) 
        myTasks = runtimeDataService.taskSummaryQuery("mary")
                .and()
                .newGroup()
                    .variableName("productCode")
                    .variableValue("Product-123")
                .endGroup()
                .newGroup()
                    .variableName("zipCode")
                    .variableValue("1233")
                .endGroup()
                .build()
                .getResultList();
        // this should not return results as zipCode value is 1234
        assertEquals(0, myTasks.size());
        
        // now let's search by two task variables all correct values 
        myTasks = runtimeDataService.taskSummaryQuery("mary")
                .and()
                .newGroup()
                    .variableName("productCode")
                    .variableValue("Product-123")
                .endGroup()
                .newGroup()
                    .variableName("zipCode")
                    .variableValue("1234")
                .endGroup()
                .build()
                .getResultList();
        // this should return results as zipCode value is 1234
        assertEquals(1, myTasks.size());
        
    }
    
    @Test
    public void testTaskVariableQueryOnBigTaskSet() {
        Random random = new Random();
        
        Map<String, Integer> numberOfInstancesPerProduct = new HashMap<String, Integer>(); 
        int i = 0;
        for (i = 0; i < 10000; i++) {
        
            int variablesIndex = random.nextInt(9);
            
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("ProductCode", productCodes.get(variablesIndex));
            params.put("Country", countries.get(variablesIndex));
            params.put("ZipCode", zipCodes.get(variablesIndex));
            params.put("Delivery", i % 2 == 0 ? true : false);
            params.put("Actor", "john,actor"+i);
            params.put("Group", "Crusaders");
            
            logger.debug("Params : " + params);
            processService.startProcess(deploymentUnitId, "task-vars.TaskWithVars", params);
            
            Integer currentValue = numberOfInstancesPerProduct.get(productCodes.get(variablesIndex));
            if (currentValue == null) {
                currentValue = 0;
            }
            numberOfInstancesPerProduct.put(productCodes.get(variablesIndex), ++currentValue);
            
        }
        logger.info("Generated {} process instances... doing searches now", i);
        logger.info("let's find tasks for product EAP only");
        long timestamp = System.currentTimeMillis();
        List<TaskSummary> myTasks = runtimeDataService.taskSummaryQuery("john")
                .variableName("productCode")
                .and()
                .variableValue("EAP")
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        assertEquals(numberOfInstancesPerProduct.get("EAP").intValue(), myTasks.size()); 
        
        logger.info("let's find tasks for product EAP or Wildfly");
        timestamp = System.currentTimeMillis();
        myTasks = runtimeDataService.taskSummaryQuery("john")
                .and()
                .variableName("productCode")
                .variableValue("EAP", "WILDFLY")
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        int total = numberOfInstancesPerProduct.get("EAP").intValue() + numberOfInstancesPerProduct.get("WILDFLY").intValue();
        assertEquals(total, myTasks.size()); 
        
        logger.info("let's find tasks for product EAP or Wildfly but take only first 10 results");
        timestamp = System.currentTimeMillis();
        myTasks = runtimeDataService.taskSummaryQuery("john")
                .and()
                .variableName("productCode")
                .variableValue("EAP", "WILDFLY")
                .maxResults(10)
                .offset(0)
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        assertEquals(10, myTasks.size()); 
        
        logger.info("let's find tasks for product EAP and country Brazil");
        timestamp = System.currentTimeMillis();
        myTasks = runtimeDataService.taskSummaryQuery("john")
                .and()
                .newGroup()
                    .variableName("productCode")
                    .variableValue("EAP")
                .endGroup()
                .newGroup()
                    .variableName("country")
                    .variableValue("Brazil")
                .endGroup()
                .maxResults(30)
                .offset(0)
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        
        
        logger.info("let's find tasks for product BPMS and BRMS by using wildcard search");
        timestamp = System.currentTimeMillis();
        myTasks = runtimeDataService.taskSummaryQuery("john")
                .and()
                .variableName("productCode")
                .regex()
                .variableValue("B*")
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        total = numberOfInstancesPerProduct.get("BPMS").intValue() + numberOfInstancesPerProduct.get("BRMS").intValue();
        assertEquals(total, myTasks.size()); 
        
        logger.info("let's find tasks for product Weblogic or WebSphere by wildcard and country Canada");
        timestamp = System.currentTimeMillis();
        myTasks = runtimeDataService.taskSummaryQuery("john")
                .and()
                .newGroup()
                    .variableName("productCode")
                    .regex()
                    .variableValue("WEB*")
                .endGroup()
                .newGroup()
                    .variableName("country")
                    .variableValue("Canada")
                .endGroup()
                .maxResults(30)
                .offset(0)
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        
        logger.info("let's find tasks for product Weblogic and WebSphere by wildcard and country starting with United");
        timestamp = System.currentTimeMillis();
        myTasks = runtimeDataService.taskSummaryQuery("john")
                .and()
                .newGroup()
                    .variableName("productCode")
                    .regex()
                    .variableValue("WEBLOGIC")
                .endGroup()
                .newGroup()
                    .variableName("productCode")
                    .variableValue("WEBSPHERE")
                .endGroup()
                .newGroup()
                    .variableName("country")
                    .regex()
                    .variableValue("United*")
                .endGroup()
                .maxResults(30)
                .offset(0)
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        assertEquals(0, myTasks.size()); // there is no way to get WEBLOGI and WEBSPHERE in United States or United Kingdom 
        
        
        logger.info("let's find tasks for product EAP and country Brazil and tasks with status Ready and Reserver");
        timestamp = System.currentTimeMillis();
        myTasks = runtimeDataService.taskSummaryQuery("john")
                .and()
                .newGroup()
                    .variableName("productCode")
                    .variableValue("EAP")
                .endGroup()
                .newGroup()
                    .variableName("country")
                    .variableValue("Brazil")
                .endGroup()
                .and()
                .status(Status.Ready, Status.Reserved)
                .maxResults(30)
                .offset(0)
                .build()
                .getResultList();
        logger.info("Task query by variable took {} ms with result size {}", (System.currentTimeMillis() - timestamp), myTasks.size());
        
    }
}
