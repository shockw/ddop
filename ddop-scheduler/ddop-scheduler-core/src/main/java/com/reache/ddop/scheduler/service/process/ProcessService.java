/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.reache.ddop.scheduler.service.process;

import com.reache.ddop.scheduler.common.enums.AuthorizationType;
import com.reache.ddop.scheduler.common.enums.TaskGroupQueueStatus;
import com.reache.ddop.scheduler.common.graph.DAG;
import com.reache.ddop.scheduler.common.model.TaskNodeRelation;
import com.reache.ddop.scheduler.common.utils.CodeGenerateUtils;
import com.reache.ddop.scheduler.dao.entity.Command;
import com.reache.ddop.scheduler.dao.entity.DagData;
import com.reache.ddop.scheduler.dao.entity.DataSource;
import com.reache.ddop.scheduler.dao.entity.DependentProcessDefinition;
import com.reache.ddop.scheduler.dao.entity.DqComparisonType;
import com.reache.ddop.scheduler.dao.entity.DqExecuteResult;
import com.reache.ddop.scheduler.dao.entity.DqRule;
import com.reache.ddop.scheduler.dao.entity.DqRuleExecuteSql;
import com.reache.ddop.scheduler.dao.entity.DqRuleInputEntry;
import com.reache.ddop.scheduler.dao.entity.Environment;
import com.reache.ddop.scheduler.dao.entity.ProcessDefinition;
import com.reache.ddop.scheduler.dao.entity.ProcessDefinitionLog;
import com.reache.ddop.scheduler.dao.entity.ProcessInstance;
import com.reache.ddop.scheduler.dao.entity.ProcessInstanceMap;
import com.reache.ddop.scheduler.dao.entity.ProcessTaskRelation;
import com.reache.ddop.scheduler.dao.entity.ProcessTaskRelationLog;
import com.reache.ddop.scheduler.dao.entity.Project;
import com.reache.ddop.scheduler.dao.entity.ProjectUser;
import com.reache.ddop.scheduler.dao.entity.Resource;
import com.reache.ddop.scheduler.dao.entity.Schedule;
import com.reache.ddop.scheduler.dao.entity.TaskDefinition;
import com.reache.ddop.scheduler.dao.entity.TaskDefinitionLog;
import com.reache.ddop.scheduler.dao.entity.TaskGroupQueue;
import com.reache.ddop.scheduler.dao.entity.TaskInstance;
import com.reache.ddop.scheduler.dao.entity.Tenant;
import com.reache.ddop.scheduler.dao.entity.UdfFunc;
import com.reache.ddop.scheduler.dao.entity.User;
import com.reache.ddop.scheduler.plugin.task.api.enums.TaskExecutionStatus;
import com.reache.ddop.scheduler.plugin.task.api.model.DateInterval;
import com.reache.ddop.scheduler.spi.enums.ResourceType;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.transaction.annotation.Transactional;

import com.reache.ddop.scheduler.service.exceptions.CronParseException;
import com.reache.ddop.scheduler.service.model.TaskNode;

public interface ProcessService {

    @Transactional
    ProcessInstance handleCommand(String host,
                                  Command command) throws CronParseException, CodeGenerateUtils.CodeGenerateException;

    void moveToErrorCommand(Command command, String message);

    int createCommand(Command command);

    List<Command> findCommandPage(int pageSize, int pageNumber);

    List<Command> findCommandPageBySlot(int pageSize, int pageNumber, int masterCount, int thisMasterSlot);

    boolean verifyIsNeedCreateCommand(Command command);

    Optional<ProcessInstance> findProcessInstanceDetailById(int processId);

    List<TaskDefinition> getTaskNodeListByDefinition(long defineCode);

    ProcessInstance findProcessInstanceById(int processId);

    ProcessDefinition findProcessDefineById(int processDefinitionId);

    ProcessDefinition findProcessDefinition(Long processDefinitionCode, int processDefinitionVersion);

    ProcessDefinition findProcessDefinitionByCode(Long processDefinitionCode);

    int deleteWorkProcessInstanceById(int processInstanceId);

    int deleteAllSubWorkProcessByParentId(int processInstanceId);

    void removeTaskLogFile(Integer processInstanceId);

    void deleteWorkTaskInstanceByProcessInstanceId(int processInstanceId);

    void recurseFindSubProcess(long parentCode, List<Long> ids);

    void createRecoveryWaitingThreadCommand(Command originCommand, ProcessInstance processInstance);

    Tenant getTenantForProcess(int tenantId, int userId);

    Environment findEnvironmentByCode(Long environmentCode);

    void setSubProcessParam(ProcessInstance subProcessInstance);

    TaskInstance submitTaskWithRetry(ProcessInstance processInstance, TaskInstance taskInstance, int commitRetryTimes,
                                     long commitInterval);

    @Transactional
    TaskInstance submitTask(ProcessInstance processInstance, TaskInstance taskInstance);

    void createSubWorkProcess(ProcessInstance parentProcessInstance, TaskInstance task);

    Map<String, String> getGlobalParamMap(String globalParams);

    Command createSubProcessCommand(ProcessInstance parentProcessInstance,
                                    ProcessInstance childInstance,
                                    ProcessInstanceMap instanceMap,
                                    TaskInstance task);

    TaskInstance submitTaskInstanceToDB(TaskInstance taskInstance, ProcessInstance processInstance);

    TaskExecutionStatus getSubmitTaskState(TaskInstance taskInstance, ProcessInstance processInstance);

    int saveCommand(Command command);

    boolean saveTaskInstance(TaskInstance taskInstance);

    boolean createTaskInstance(TaskInstance taskInstance);

    boolean updateTaskInstance(TaskInstance taskInstance);

    TaskInstance findTaskInstanceById(Integer taskId);

    List<TaskInstance> findTaskInstanceByIdList(List<Integer> idList);

    void packageTaskInstance(TaskInstance taskInstance, ProcessInstance processInstance);

    void updateTaskDefinitionResources(TaskDefinition taskDefinition);

    List<Integer> findTaskIdByInstanceState(int instanceId, TaskExecutionStatus state);

    List<TaskInstance> findValidTaskListByProcessId(Integer processInstanceId);

    List<TaskInstance> findPreviousTaskListByWorkProcessId(Integer processInstanceId);

    int updateWorkProcessInstanceMap(ProcessInstanceMap processInstanceMap);

    int createWorkProcessInstanceMap(ProcessInstanceMap processInstanceMap);

    ProcessInstanceMap findWorkProcessMapByParent(Integer parentWorkProcessId, Integer parentTaskId);

    int deleteWorkProcessMapByParentId(int parentWorkProcessId);

    ProcessInstance findSubProcessInstance(Integer parentProcessId, Integer parentTaskId);

    ProcessInstance findParentProcessInstance(Integer subProcessId);

    void changeOutParam(TaskInstance taskInstance);

    Schedule querySchedule(int id);

    List<Schedule> queryReleaseSchedulerListByProcessDefinitionCode(long processDefinitionCode);

    Map<Long, String> queryWorkerGroupByProcessDefinitionCodes(List<Long> processDefinitionCodeList);

    List<DependentProcessDefinition> queryDependentProcessDefinitionByProcessDefinitionCode(long processDefinitionCode);

    List<ProcessInstance> queryNeedFailoverProcessInstances(String host);

    List<String> queryNeedFailoverProcessInstanceHost();

    @Transactional
    void processNeedFailoverProcessInstances(ProcessInstance processInstance);

    List<TaskInstance> queryNeedFailoverTaskInstances(String host);

    DataSource findDataSourceById(int id);

    ProcessInstance findProcessInstanceByTaskId(int taskId);

    List<UdfFunc> queryUdfFunListByIds(Integer[] ids);

    String queryTenantCodeByResName(String resName, ResourceType resourceType);

    List<Schedule> selectAllByProcessDefineCode(long[] codes);

    ProcessInstance findLastSchedulerProcessInterval(Long definitionCode, DateInterval dateInterval);

    ProcessInstance findLastManualProcessInterval(Long definitionCode, DateInterval dateInterval);

    ProcessInstance findLastRunningProcess(Long definitionCode, Date startTime, Date endTime);

    String queryUserQueueByProcessInstance(ProcessInstance processInstance);

    ProjectUser queryProjectWithUserByProcessInstanceId(int processInstanceId);

    String getTaskWorkerGroup(TaskInstance taskInstance);

    List<Project> getProjectListHavePerm(int userId);

    <T> List<T> listUnauthorized(int userId, T[] needChecks, AuthorizationType authorizationType);

    User getUserById(int userId);

    Resource getResourceById(int resourceId);

    List<Resource> listResourceByIds(Integer[] resIds);

    String formatTaskAppId(TaskInstance taskInstance);

    int switchVersion(ProcessDefinition processDefinition, ProcessDefinitionLog processDefinitionLog);

    int switchProcessTaskRelationVersion(ProcessDefinition processDefinition);

    int switchTaskDefinitionVersion(long taskCode, int taskVersion);

    String getResourceIds(TaskDefinition taskDefinition);

    int saveTaskDefine(User operator, long projectCode, List<TaskDefinitionLog> taskDefinitionLogs, Boolean syncDefine);

    int saveProcessDefine(User operator, ProcessDefinition processDefinition, Boolean syncDefine,
                          Boolean isFromProcessDefine);

    int saveTaskRelation(User operator, long projectCode, long processDefinitionCode, int processDefinitionVersion,
                         List<ProcessTaskRelationLog> taskRelationList, List<TaskDefinitionLog> taskDefinitionLogs,
                         Boolean syncDefine);

    boolean isTaskOnline(long taskCode);

    DAG<String, TaskNode, TaskNodeRelation> genDagGraph(ProcessDefinition processDefinition);

    DagData genDagData(ProcessDefinition processDefinition);

    List<TaskDefinitionLog> genTaskDefineList(List<ProcessTaskRelation> processTaskRelations);

    List<TaskDefinitionLog> getTaskDefineLogListByRelation(List<ProcessTaskRelation> processTaskRelations);

    TaskDefinition findTaskDefinition(long taskCode, int taskDefinitionVersion);

    List<ProcessTaskRelation> findRelationByCode(long processDefinitionCode, int processDefinitionVersion);

    List<TaskNode> transformTask(List<ProcessTaskRelation> taskRelationList,
                                 List<TaskDefinitionLog> taskDefinitionLogs);

    Map<ProcessInstance, TaskInstance> notifyProcessList(int processId);

    DqExecuteResult getDqExecuteResultByTaskInstanceId(int taskInstanceId);

    int updateDqExecuteResultUserId(int taskInstanceId);

    int updateDqExecuteResultState(DqExecuteResult dqExecuteResult);

    int deleteDqExecuteResultByTaskInstanceId(int taskInstanceId);

    int deleteTaskStatisticsValueByTaskInstanceId(int taskInstanceId);

    DqRule getDqRule(int ruleId);

    List<DqRuleInputEntry> getRuleInputEntry(int ruleId);

    List<DqRuleExecuteSql> getDqExecuteSql(int ruleId);

    DqComparisonType getComparisonTypeById(int id);

    boolean acquireTaskGroup(int taskId,
                             String taskName, int groupId,
                             int processId, int priority);

    boolean robTaskGroupResource(TaskGroupQueue taskGroupQueue);

    void releaseAllTaskGroup(int processInstanceId);

    TaskInstance releaseTaskGroup(TaskInstance taskInstance);

    void changeTaskGroupQueueStatus(int taskId, TaskGroupQueueStatus status);

    TaskGroupQueue insertIntoTaskGroupQueue(Integer taskId,
                                            String taskName,
                                            Integer groupId,
                                            Integer processId,
                                            Integer priority,
                                            TaskGroupQueueStatus status);

    int updateTaskGroupQueueStatus(Integer taskId, int status);

    int updateTaskGroupQueue(TaskGroupQueue taskGroupQueue);

    TaskGroupQueue loadTaskGroupQueue(int taskId);

    void sendStartTask2Master(ProcessInstance processInstance, int taskId,
                              com.reache.ddop.scheduler.remote.command.CommandType taskType);

    ProcessInstance loadNextProcess4Serial(long code, int state, int id);

    public String findConfigYamlByName(String clusterName);

    void forceProcessInstanceSuccessByTaskInstanceId(Integer taskInstanceId);
}
