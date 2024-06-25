/*
 * Copyright 2024 Conductor Authors. <p> Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at <p> http://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package com.netflix.conductor.mongo.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.exception.NonTransientException;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.mongo.entities.EventExecutionDocument;
import com.netflix.conductor.mongo.entities.PollDataDocument;
import com.netflix.conductor.mongo.entities.TaskDataDocument;
import com.netflix.conductor.mongo.entities.TaskInProgressDocument;
import com.netflix.conductor.mongo.entities.TaskScheduledDocument;
import com.netflix.conductor.mongo.entities.WorkflowDefToWorkflowDocument;
import com.netflix.conductor.mongo.entities.WorkflowDocument;
import com.netflix.conductor.mongo.entities.WorkflowPendingDocument;
import com.netflix.conductor.mongo.entities.WorkflowToTaskDocument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@Trace
public class MongoExecutionDAO extends MongoBaseDAO
        implements ExecutionDAO, RateLimitingDAO, PollDataDAO, ConcurrentExecutionLimitDAO {

    private final MongoTemplate mongoTemplate;

    public MongoExecutionDAO(ObjectMapper objectMapper, MongoTemplate mongoTemplate) {
        super(objectMapper);
        this.mongoTemplate = mongoTemplate;
    }

    /*
     * -------------------------------------------------------------------------------------------
     * ExecutionDAO Interface
     * -------------------------------------------------------------------------------------------
     */

    /**
     * @param taskName Name of the task
     * @param workflowId Workflow instance id
     * @return List of pending tasks (in_progress)
     */
    @Override
    public List<TaskModel> getPendingTasksByWorkflow(String taskName, String workflowId) {

        List<TaskModel> result = new ArrayList<>();

        List<TaskModel> tasks = getTasksForWorkflow(workflowId);
        return tasks.stream().filter(task -> taskName.equals(task.getTaskType()))
                .filter(task -> TaskModel.Status.IN_PROGRESS.equals(task.getStatus()))
                .collect(Collectors.toList());

        // TODO
        // Query searchQuery = new Query();
        // searchQuery.addCriteria(Criteria.where("task_name").is(taskName)
        // .andOperator(Criteria.where("workflow_id").is(workflowId)));

        // List<TaskInProgressDocument> inProgress =
        // mongoTemplate.find(searchQuery, TaskInProgressDocument.class);

        // if (!inProgress.isEmpty()) {

        // Criteria orCriteria = new Criteria();
        // Criteria expression = new Criteria();
        // inProgress.forEach(value -> expression.and("task_id").is(value));

        // List<Criteria> orExpression = new ArrayList<>();
        // orExpression.add(expression);

        // Query orQuery = new Query();
        // orQuery.addCriteria(
        // orCriteria.orOperator(orExpression.toArray(new Criteria[orExpression.size()])));

        // mongoTemplate.find(orQuery, TaskDataDocument.class).forEach(tdd -> {
        // char[] j = tdd.getJson_data();
        // var x = readValue(j, Task.class);
        // result.add(readValue(tdd.getJson_data(), Task.class));
        // });
        // }

        // return result;
    }

    /**
     * @param taskType Type of task
     * @param startKey start
     * @param count number of tasks to return
     * @return List of tasks starting from startKey
     */
    @Override
    public List<TaskModel> getTasks(String taskType, String startKey, int count) {

        List<TaskModel> tasks = new ArrayList<>(count);
        List<TaskModel> pendingTasks = getPendingTasksForTaskType(taskType);
        boolean startKeyFound = startKey == null;
        int found = 0;

        for (TaskModel pendingTask : pendingTasks) {
            if (!startKeyFound) {
                if (pendingTask.getTaskId().equals(startKey)) {
                    startKeyFound = true;
                    if (startKey != null) {
                        continue;
                    }
                }
            }
            if (startKeyFound && found < count) {
                tasks.add(pendingTask);
                found++;
            }
        }

        return tasks;
    }

    /**
     * @param tasks tasks to be created
     * @return List of tasks that were created.
     *         <p>
     *         <b>Note on the primary key constraint</b>
     *         <p>
     *         For a given task reference name and retryCount should be considered unique/primary
     *         key. Given two tasks with the same reference name and retryCount only one should be
     *         added to the database.
     */
    @Override
    public List<TaskModel> createTasks(List<TaskModel> tasks) {

        List<TaskModel> created = Lists.newArrayListWithCapacity(tasks.size());

        for (TaskModel task : tasks) {
            try {
                validate(task);
            } catch (NullPointerException npe) {
                throw new NonTransientException(npe.getMessage(), npe);
            }

            task.setScheduledTime(System.currentTimeMillis());

            final String taskKey = taskKey(task);

            boolean scheduledTaskAdded = addScheduledTask(task, taskKey);

            if (!scheduledTaskAdded) {
                logger.trace("Task already scheduled, skipping the run " + task.getTaskId()
                        + ", ref=" + task.getReferenceTaskName() + ", key=" + taskKey);
                continue;
            }

            insertOrUpdateTaskData(task);
            addWorkflowToTaskMapping(task);
            addTaskInProgress(task);
            updateTask(task);
            created.add(task);
        }

        return created;
    }

    /**
     * @param task Task to be updated
     */
    @Override
    public void updateTask(TaskModel task) {

        Optional<TaskDef> taskDefinition = task.getTaskDefinition();

        if (taskDefinition.isPresent() && taskDefinition.get().concurrencyLimit() > 0) {
            boolean inProgress = task.getStatus() != null
                    && task.getStatus().equals(TaskModel.Status.IN_PROGRESS);
            updateInProgressStatus(task, inProgress);
        }

        insertOrUpdateTaskData(task);

        if (task.getStatus() != null && task.getStatus().isTerminal()) {
            removeTaskInProgress(task);
        }

        addWorkflowToTaskMapping(task);
    }

    /**
     * @param taskId id of the task to be removed.
     * @return true if the deletion is successful, false otherwise.
     */
    @Override
    public boolean removeTask(String taskId) {

        TaskModel task = getTask(taskId);

        if (task == null) {
            logger.warn("No such task found by id {}", taskId);
            return false;
        }

        final String taskKey = taskKey(task);

        removeScheduledTask(task, taskKey);
        removeWorkflowToTaskMapping(task);
        removeTaskInProgress(task);
        removeTaskData(task);

        return true;
    }

    /**
     * @param taskId Task instance id
     * @return Task
     */
    @Override
    public TaskModel getTask(String taskId) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("task_id").is(taskId));
        TaskDataDocument taskDocument = mongoTemplate.findOne(searchQuery, TaskDataDocument.class);
        return null != taskDocument && null != taskDocument.getJson_data() ? readValue(
                mongoTemplate.findOne(searchQuery, TaskDataDocument.class).getJson_data(),
                TaskModel.class) : null;
    }

    /**
     * @param taskIds Task instance ids
     * @return List of tasks
     */
    @Override
    public List<TaskModel> getTasks(List<String> taskIds) {

        List<TaskModel> result = new ArrayList<TaskModel>();

        try {
            if (!taskIds.isEmpty()) {
                Query orQuery = new Query();
                Criteria orCriteria = new Criteria();
                List<Criteria> orExpression = new ArrayList<>();
                for (String taskId : taskIds) {
                    Criteria expression = Criteria.where("task_id").is(taskId);
                    orExpression.add(expression);
                }
                orQuery.addCriteria(orCriteria
                        .orOperator(orExpression.toArray(new Criteria[orExpression.size()])));
                List<TaskDataDocument> taskDataList =
                        mongoTemplate.find(orQuery, TaskDataDocument.class);

                if (taskDataList.isEmpty())
                    return result;
                else
                    taskDataList.forEach(taskDoc -> {
                        if (taskDoc.getJson_data() != null)
                            result.add(readValue(taskDoc.getJson_data(), TaskModel.class));
                    });
            }
        } catch (Exception e) {
            e.printStackTrace();
            taskIds.forEach(taskId -> {
                result.add(getTask(taskId));
            });
        }
        return result;
    }

    /**
     * @param taskType Type of the task for which to retrieve the list of pending tasks
     * @return List of pending tasks
     */
    @Override
    public List<TaskModel> getPendingTasksForTaskType(String taskType) {

        List<TaskModel> result = new ArrayList<TaskModel>();

        Query searchQuery = new Query();
        // TODO
        searchQuery.addCriteria(Criteria.where("task_name").is(taskType));

        List<TaskInProgressDocument> inProgress =
                mongoTemplate.find(searchQuery, TaskInProgressDocument.class);

        if (!inProgress.isEmpty()) {
            Query orQuery = new Query();
            Criteria orCriteria = new Criteria();
            List<Criteria> orExpression = new ArrayList<>();
            for (TaskInProgressDocument aTaskInProgressDocument : inProgress) {
                Criteria expression =
                        Criteria.where("task_id").is(aTaskInProgressDocument.getTask_id());
                orExpression.add(expression);
            }
            orQuery.addCriteria(
                    orCriteria.orOperator(orExpression.toArray(new Criteria[orExpression.size()])));
            List<TaskDataDocument> taskDataList =
                    mongoTemplate.find(orQuery, TaskDataDocument.class);

            if (taskDataList.isEmpty())
                return result;
            else
                taskDataList.forEach(taskDoc -> {
                    if (taskDoc.getJson_data() != null)
                        result.add(readValue(taskDoc.getJson_data(), TaskModel.class));
                });
        }

        return result;
    }

    /**
     * @param workflowId Workflow instance id
     * @return List of tasks for the given workflow instance id
     */
    @Override
    public List<TaskModel> getTasksForWorkflow(String workflowId) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_id").is(workflowId));
        List<String> taskIds = new ArrayList<String>();

        mongoTemplate.find(searchQuery, WorkflowToTaskDocument.class)
                .forEach(wttd -> taskIds.add(wttd.getTask_id()));

        return getTasks(taskIds);
    }

    /**
     * @param workflow Workflow to be created
     * @return Id of the newly created workflow
     */
    @Override
    public String createWorkflow(WorkflowModel workflow) {
        return insertOrUpdateWorkflow(workflow, false);
    }

    /**
     * @param workflow Workflow to be updated
     * @return Id of the updated workflow
     */
    @Override
    public String updateWorkflow(WorkflowModel workflow) {
        return insertOrUpdateWorkflow(workflow, true);
    }

    /**
     * @param workflowId workflow instance id
     * @return true if the deletion is successful, false otherwise
     */
    @Override
    public boolean removeWorkflow(String workflowId) {

        boolean removed = false;
        WorkflowModel workflow = getWorkflow(workflowId, true);
        if (workflow != null) {

            removeWorkflowDefToWorkflowMapping(workflow);
            removePendingWorkflow(workflow.getWorkflowName(), workflowId);

            removed = true;

            for (TaskModel task : workflow.getTasks()) {
                if (!removeTask(task.getTaskId())) {
                    removed = false;
                }
            }
        }
        return removed;
    }

    /**
     * Removes the workflow with ttl seconds
     *
     * @param workflowId workflowId workflow instance id
     * @param ttlSeconds time to live in seconds.
     * @return
     */
    @Override
    public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
        throw new UnsupportedOperationException(
                "This method is not implemented in MongoExecutionDAO. Please use RedisDAO mode instead for using TTLs.");
    }

    /**
     * @param workflowType Workflow Type
     * @param workflowId workflow instance id
     */
    @Override
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {
        removePendingWorkflow(workflowType, workflowId);
    }

    /**
     * @param workflowId workflow instance id
     * @return Workflow
     */
    @Override
    public WorkflowModel getWorkflow(String workflowId) {
        return getWorkflow(workflowId, true);
    }

    /**
     * @param workflowId workflow instance id
     * @param includeTasks if set, includes the tasks (pending and completed) sorted by Task
     *        Sequence number in Workflow.
     * @return Workflow instance details
     */
    @Override
    public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {

        WorkflowModel workflow = readWorkflow(workflowId);

        if (workflow != null) {
            if (includeTasks) {
                List<TaskModel> tasks = getTasksForWorkflow(workflowId);
                tasks.sort(Comparator.comparingLong(TaskModel::getScheduledTime)
                        .thenComparingInt(TaskModel::getSeq));
                workflow.setTasks(tasks);
            }
        }
        return workflow;
    }

    /**
     * @param workflowName name of the workflow
     * @param version the workflow version
     * @return List of workflow ids which are running
     */
    @Override
    public List<String> getRunningWorkflowIds(String workflowName, int version) {

        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");

        List<String> workflowIds = new ArrayList<String>();
        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_type").is(workflowName));

        mongoTemplate.find(searchQuery, WorkflowPendingDocument.class)
                .forEach(wpd -> workflowIds.add(wpd.getWorkflow_id()));

        return workflowIds;
    }

    /**
     * @param workflowName Name of the workflow
     * @param version the workflow version
     * @return List of workflows that are running
     */
    @Override
    public List<WorkflowModel> getPendingWorkflowsByType(String workflowName, int version) {

        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        return getRunningWorkflowIds(workflowName, version).stream().map(this::getWorkflow)
                .filter(workflow -> workflow.getWorkflowVersion() == version)
                .collect(Collectors.toList());
    }

    /**
     * @param workflowName Name of the workflow
     * @return No. of running workflows
     */
    @Override
    public long getPendingWorkflowCount(String workflowName) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_type").is(workflowName));

        return mongoTemplate.count(searchQuery, WorkflowPendingDocument.class);
    }

    /**
     * @param taskDefName Name of the task
     * @return Number of task currently in IN_PROGRESS status
     */
    @Override
    public long getInProgressTaskCount(String taskDefName) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(
                Criteria.where("task_def_name").is(taskDefName).and("in_progress_status").is(true));

        return mongoTemplate.count(searchQuery, TaskInProgressDocument.class);
    }

    /**
     * @param workflowName Name of the workflow
     * @param startTime epoch time
     * @param endTime epoch time
     * @return List of workflows between start and end time
     */
    @Override
    public List<WorkflowModel> getWorkflowsByType(String workflowName, Long startTime,
            Long endTime) {

        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        Preconditions.checkNotNull(startTime, "startTime cannot be null");
        Preconditions.checkNotNull(endTime, "endTime cannot be null");

        List<WorkflowModel> workflows = new LinkedList<WorkflowModel>();

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_def").is(workflowName));
        searchQuery.addCriteria(Criteria.where("date_str").gte(startTime).lte(endTime));

        List<String> workflowIds = new ArrayList<String>();
        mongoTemplate.find(searchQuery, WorkflowDefToWorkflowDocument.class)
                .forEach(wdtw -> workflowIds.add(wdtw.getWorkflow_id()));

        workflowIds.forEach(workflowId -> {
            try {
                WorkflowModel wf = getWorkflow(workflowId);
                if (wf.getCreateTime() >= startTime && wf.getCreateTime() <= endTime) {
                    workflows.add(wf);
                }
            } catch (Exception e) {
                logger.error("Unable to load workflow id {} with name {}", workflowId, workflowName,
                        e);
            }
        });

        return workflows;
    }

    /**
     * @param workflowName workflow name
     * @param correlationId Correlation Id
     * @param includeTasks Option to includeTasks in results
     * @return List of workflows by correlation id
     */
    @Override
    public List<WorkflowModel> getWorkflowsByCorrelationId(String workflowName,
            String correlationId, boolean includeTasks) {

        Preconditions.checkNotNull(correlationId, "correlationId cannot be null");
        List<WorkflowModel> result = new ArrayList<WorkflowModel>();

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_def").is(workflowName));

        List<WorkflowDefToWorkflowDocument> inProgress =
                mongoTemplate.find(searchQuery, WorkflowDefToWorkflowDocument.class);

        if (!inProgress.isEmpty()) {

            Query orQuery = new Query();
            Criteria orCriteria = new Criteria();
            List<Criteria> orExpression = new ArrayList<>();
            for (WorkflowDefToWorkflowDocument aWorkflowDefToWorkflowDocument : inProgress) {
                Criteria expression = Criteria.where("workflow_id")
                        .is(aWorkflowDefToWorkflowDocument.getWorkflow_id()).and("correlation_id")
                        .is(correlationId);
                orExpression.add(expression);
            }
            orQuery.addCriteria(
                    orCriteria.orOperator(orExpression.toArray(new Criteria[orExpression.size()])));
            mongoTemplate.find(orQuery, WorkflowDocument.class).forEach(wd -> {
                result.add(readValue(wd.getJson_data(), WorkflowModel.class));
            });
        }

        return result;
    }

    /**
     * @return true, if the DAO implementation is capable of searching across workflows false, if
     *         the DAO implementation cannot perform searches across workflows (and needs to use
     *         indexDAO)
     */
    @Override
    public boolean canSearchAcrossWorkflows() {
        return true;
    }

    // Events

    /**
     * @param eventExecution Event Execution to be stored
     * @return true if the event was added. false otherwise when the event by id is already already
     *         stored.
     */
    @Override
    public boolean addEventExecution(EventExecution eventExecution) {

        try {
            return insertEventExecution(eventExecution);
        } catch (Exception e) {
            throw new NonTransientException(
                    "Unable to add event execution " + eventExecution.getId(), e);
        }
    }

    /**
     * @param eventExecution Event execution to be updated
     */
    @Override
    public void updateEventExecution(EventExecution eventExecution) {

        Query updateQuery = new Query();
        updateQuery.addCriteria(Criteria.where("event_handler_name").is(eventExecution.getName())
                .and("event_name").is(eventExecution.getEvent()).and("message_id")
                .is(eventExecution.getMessageId()).and("execution_id").is(eventExecution.getId()));

        Update update = new Update();
        update.set("json_data", toJson(eventExecution));
        mongoTemplate.updateMulti(updateQuery, update, EventExecutionDocument.class);
    }

    /**
     * @param eventExecution Event execution to be removed
     */
    @Override
    public void removeEventExecution(EventExecution eventExecution) {

        Query deleteQuery = new Query();
        deleteQuery.addCriteria(Criteria.where("event_handler_name").is(eventExecution.getName())
                .and("event_name").is(eventExecution.getEvent()).and("message_id")
                .is(eventExecution.getMessageId()).and("execution_id").is(eventExecution.getId()));

        mongoTemplate.remove(deleteQuery, EventExecutionDocument.class);
    }

    /*
     * -------------------------------------------------------------------------------------------
     * RateLimitingDAO Interface
     * -------------------------------------------------------------------------------------------
     */

    /**
     * * This method is not implemented for MongoDB backed Conductor
     *
     * <p>
     * Checks if the Task is rate limited or not based on the
     * {@link TaskModel#getRateLimitPerFrequency()} and
     * {@link TaskModel#getRateLimitFrequencyInSeconds()}
     *
     * @param task: which needs to be evaluated whether it is rateLimited or not
     * @return true: If the {@link TaskModel} is rateLimited false: If the {@link TaskModel} is not
     *         rateLimited
     */
    @Override
    public boolean exceedsRateLimitPerFrequency(TaskModel task, TaskDef taskDef) {
        return false;
    }

    /*
     * -------------------------------------------------------------------------------------------
     * PollDataDAO Interface
     * -------------------------------------------------------------------------------------------
     */

    /**
     * Updates the {@link PollData} information with the most recently polled data for a task queue.
     *
     * @param taskDefName name of the task as specified in the task definition
     * @param domain domain in which this task is being polled from
     * @param workerId the identifier of the worker polling for this task
     */
    @Override
    public void updateLastPollData(String taskDefName, String domain, String workerId) {
        Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
        PollData pollData = new PollData(taskDefName, domain, workerId, System.currentTimeMillis());
        String effectiveDomain = (domain == null) ? "DEFAULT" : domain;
        insertOrUpdatePollData(pollData, effectiveDomain);
    }

    /**
     * Retrieve the {@link PollData} for the given task in the given domain.
     *
     * @param taskDefName name of the task as specified in the task definition
     * @param domain domain for which {@link PollData} is being requested
     * @return the {@link PollData} for the given task queue in the specified domain
     */
    @Override
    public PollData getPollData(String taskDefName, String domain) {
        Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
        String effectiveDomain = (domain == null) ? "DEFAULT" : domain;
        return readPollData(taskDefName, effectiveDomain);
    }

    /**
     * Retrieve the {@link PollData} for the given task across all domains.
     *
     * @param taskDefName name of the task as specified in the task definition
     * @return the {@link PollData} for the given task queue in all domains
     */
    @Override
    public List<PollData> getPollData(String taskDefName) {
        Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
        return readAllPollData(taskDefName);
    }

    /**
     * Retrieve the {@link PollData} for all task types
     *
     * @return the {@link PollData} for all task types
     */
    @Override
    public List<PollData> getAllPollData() {
        try {
            List<PollData> result = new ArrayList<>();

            Query query = new Query();
            query.with(Sort.by(Sort.Direction.ASC, "queue_name"));
            // TODO
            mongoTemplate.find(query, PollDataDocument.class)
                    .forEach(doc -> result.add(readValue(doc.getJson_data(), PollData.class)));

            return result;

        } catch (Exception e) {
            throw new NonTransientException(e.getMessage(), e);
        }
    }

    /*
     * -------------------------------------------------------------------------------------------
     * ConcurrentExecutionLimitDAO Interface
     * -------------------------------------------------------------------------------------------
     */

    /**
     * This method is not implemented for MongoDB backed Conductor
     *
     * @param task The task to be executed
     */
    @Override
    public void addTaskToLimit(TaskModel task) {
        throw new UnsupportedOperationException(
                getClass() + " does not support addTaskToLimit method.");
    }

    /**
     * This method is not implemented for MongoDB backed Conductor
     *
     * @param task The task to be executed
     */
    @Override
    public void removeTaskFromLimit(TaskModel task) {
        throw new UnsupportedOperationException(
                getClass() + " does not support removeTaskFromLimit method.");
    }

    /**
     * Checks if the number of tasks in progress for the given taskDef will exceed the limit if the
     * task is scheduled to be in progress (given to the worker or for system tasks start() method
     * called)
     *
     * @param task The task to be executed. Limit is set in the Task's definition
     * @return true if by executing this task, the limit is breached. false otherwise.
     * @see TaskDef#concurrencyLimit()
     */
    @Override
    public boolean exceedsLimit(TaskModel task) {

        Optional<TaskDef> taskDefinition = task.getTaskDefinition();
        if (!taskDefinition.isPresent()) {
            return false;
        }

        TaskDef taskDef = taskDefinition.get();

        int limit = taskDef.concurrencyLimit();
        if (limit <= 0) {
            return false;
        }

        long current = getInProgressTaskCount(task.getTaskDefName());

        if (current >= limit) {
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
            return true;
        }

        logger.info("Task execution count for {}: limit={}, current={}", task.getTaskDefName(),
                limit, getInProgressTaskCount(task.getTaskDefName()));

        String taskId = task.getTaskId();

        List<String> tasksInProgressInOrderOfArrival =
                findAllTasksInProgressInOrderOfArrival(task, limit);
        boolean rateLimited = !tasksInProgressInOrderOfArrival.contains(taskId);
        if (rateLimited) {
            logger.info("Task execution count limited. {}, limit {}, current {}",
                    task.getTaskDefName(), limit, getInProgressTaskCount(task.getTaskDefName()));
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
        }
        return rateLimited;
    }

    /*
     * -------------------------------------------------------------------------------------------
     */

    private static String dateStr(Long timeInMs) {

        Date date = new Date(timeInMs);
        return dateStr(date);
    }

    private static String dateStr(Date date) {

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.format(date);
    }

    private static String taskKey(TaskModel task) {

        return task.getReferenceTaskName() + "_" + task.getRetryCount();
    }

    private void updateWorkflowToDB(WorkflowModel workflow) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_id").is(workflow.getWorkflowId()));

        Update update = new Update();
        update.set("json_data", toJson(workflow));
        mongoTemplate.updateMulti(searchQuery, update, WorkflowDocument.class);
    }

    @VisibleForTesting
    boolean addScheduledTask(TaskModel task, String taskKey) {

        Query checkExistsQuery = new Query();
        checkExistsQuery.addCriteria(Criteria.where("workflow_id").is(task.getWorkflowInstanceId())
                .and("task_key").is(taskKey));

        boolean exists = mongoTemplate.exists(checkExistsQuery, TaskScheduledDocument.class);

        if (!exists) {
            TaskScheduledDocument newTaskScheduledDocument = new TaskScheduledDocument();
            newTaskScheduledDocument.setWorkflow_id(task.getWorkflowInstanceId());
            newTaskScheduledDocument.setTask_key(taskKey);
            newTaskScheduledDocument.setTask_id(newTaskScheduledDocument.getTask_id());
            newTaskScheduledDocument = mongoTemplate.save(newTaskScheduledDocument);
            return null != newTaskScheduledDocument;
        } else {
            return false;
        }
    }

    private void insertOrUpdateTaskData(TaskModel task) {

        Query updateQuery = new Query();
        updateQuery.addCriteria(Criteria.where("task_id").is(task.getTaskId()));
        Update update = new Update();
        update.set("json_data", toJson(task));
        update.set("task_id", task.getTaskId());
        mongoTemplate.upsert(updateQuery, update, TaskDataDocument.class);
    }

    private void removeTaskData(TaskModel task) {

        Query query = new Query();
        query.addCriteria(Criteria.where("task_id").is(task.getTaskId()));
        mongoTemplate.remove(query, TaskDataDocument.class);
    }

    private void addWorkflowToTaskMapping(TaskModel task) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_id").is(task.getWorkflowInstanceId())
                .and("task_id").is(task.getTaskId()));

        boolean exists = mongoTemplate.exists(searchQuery, WorkflowToTaskDocument.class);

        if (!exists) {
            WorkflowToTaskDocument newWorkflowToTaskDocument = new WorkflowToTaskDocument();
            newWorkflowToTaskDocument.setWorkflow_id(task.getWorkflowInstanceId());
            newWorkflowToTaskDocument.setTask_id(task.getTaskId());
            newWorkflowToTaskDocument = mongoTemplate.save(newWorkflowToTaskDocument);
        }
    }

    private void validate(TaskModel task) {
        Preconditions.checkNotNull(task, "task object cannot be null");
        Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
        Preconditions.checkNotNull(task.getWorkflowInstanceId(),
                "Workflow instance id cannot be null");
        Preconditions.checkNotNull(task.getReferenceTaskName(),
                "Task reference name cannot be null");
    }

    private void addTaskInProgress(TaskModel task) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("task_def_name").is(task.getTaskDefName())
                .and("task_id").is(task.getTaskId()));
        mongoTemplate.count(searchQuery, TaskInProgressDocument.class);

        boolean exists = mongoTemplate.exists(searchQuery, TaskInProgressDocument.class);

        if (!exists) {
            TaskInProgressDocument newTaskInProgressDocument = new TaskInProgressDocument();
            newTaskInProgressDocument.setTask_def_name(task.getTaskDefName());
            newTaskInProgressDocument.setTask_id(task.getTaskId());
            newTaskInProgressDocument.setWorkflow_id(task.getWorkflowInstanceId());
            newTaskInProgressDocument = mongoTemplate.save(newTaskInProgressDocument);
        }
    }

    private void updateInProgressStatus(TaskModel task, boolean inProgress) {

        Query updateQuery = new Query();
        updateQuery.addCriteria(Criteria.where("task_def_name").is(task.getTaskDefName())
                .and("task_id").is(task.getTaskId()));

        Update update = new Update();
        update.set("in_progress_status", inProgress);
        mongoTemplate.updateFirst(updateQuery, update, TaskInProgressDocument.class);
    }

    private void removeTaskInProgress(TaskModel task) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("task_def_name").is(task.getTaskDefName())
                .and("task_id").is(task.getTaskId()));
        mongoTemplate.remove(searchQuery, TaskInProgressDocument.class);
    }

    private List<String> findAllTasksInProgressInOrderOfArrival(TaskModel task, int limit) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("task_def_name").is(task.getTaskDefName()));
        searchQuery.with(Sort.by(Order.asc("created_on")));
        searchQuery.limit(limit);

        List<String> tips = new ArrayList<String>();
        mongoTemplate.find(searchQuery, TaskInProgressDocument.class)
                .forEach(tip -> tips.add(tip.getTask_id()));
        return tips;
    }

    private void removeScheduledTask(TaskModel task, String taskKey) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_id").is(task.getWorkflowInstanceId())
                .and("task_key").is(taskKey));
        mongoTemplate.remove(searchQuery, TaskScheduledDocument.class);
    }

    private void removeWorkflowToTaskMapping(TaskModel task) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_id").is(task.getWorkflowInstanceId())
                .and("task_id").is(task.getTaskId()));
        mongoTemplate.remove(searchQuery, WorkflowToTaskDocument.class);
    }

    private String insertOrUpdateWorkflow(WorkflowModel workflow, boolean update) {

        try {
            Preconditions.checkNotNull(workflow, "workflow object cannot be null");
        } catch (NullPointerException npe) {
            throw new NonTransientException(npe.getMessage(), npe);
        }

        boolean terminal = workflow.getStatus().isTerminal();

        List<TaskModel> tasks = workflow.getTasks();
        workflow.setTasks(Lists.newLinkedList());

        if (!update) {
            addWorkflow(workflow);
            addWorkflowDefToWorkflowMapping(workflow);
        } else {
            updateWorkflowToDB(workflow);
        }

        if (terminal) {
            removePendingWorkflow(workflow.getWorkflowName(), workflow.getWorkflowId());
        } else {
            addPendingWorkflow(workflow.getWorkflowName(), workflow.getWorkflowId());
        }

        workflow.setTasks(tasks);
        return workflow.getWorkflowId();
    }

    private void addWorkflow(WorkflowModel workflow) {

        WorkflowDocument workflowDocument = new WorkflowDocument();
        workflowDocument.setWorkflow_id(workflow.getWorkflowId());
        workflowDocument.setCorrelation_id(workflow.getCorrelationId());
        workflowDocument.setJson_data(toJson(workflow));
        workflowDocument = mongoTemplate.save(workflowDocument);
    }

    private void addWorkflowDefToWorkflowMapping(WorkflowModel workflow) {

        WorkflowDefToWorkflowDocument workflowDefToWorkflowDocument =
                new WorkflowDefToWorkflowDocument();
        workflowDefToWorkflowDocument.setDate_str(dateStr(workflow.getCreateTime()));
        workflowDefToWorkflowDocument.setWorkflow_def(workflow.getWorkflowName());
        workflowDefToWorkflowDocument.setWorkflow_id(workflow.getWorkflowId());
        workflowDefToWorkflowDocument = mongoTemplate.save(workflowDefToWorkflowDocument);
    }

    private void removePendingWorkflow(String workflowType, String workflowId) {

        Query deleteQuery = new Query();
        deleteQuery.addCriteria(
                Criteria.where("workflow_type").is(workflowType).and("workflow_id").is(workflowId));
        mongoTemplate.remove(deleteQuery, WorkflowPendingDocument.class);
    }

    private void addPendingWorkflow(String workflowType, String workflowId) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(
                Criteria.where("workflow_type").is(workflowType).and("workflow_id").is(workflowId));

        boolean exists = mongoTemplate.exists(searchQuery, WorkflowPendingDocument.class);

        if (!exists) {
            WorkflowPendingDocument workflowPendingDocument = new WorkflowPendingDocument();
            workflowPendingDocument.setWorkflow_id(workflowId);
            workflowPendingDocument.setWorkflow_type(workflowType);
            workflowPendingDocument = mongoTemplate.save(workflowPendingDocument);
        }
    }

    private void removeWorkflowDefToWorkflowMapping(WorkflowModel workflow) {

        Query deleteQuery = new Query();
        deleteQuery.addCriteria(Criteria.where("workflow_def").is(workflow.getWorkflowName())
                .and("date_str").is(dateStr(workflow.getCreateTime())).and("workflow_id")
                .is(workflow.getWorkflowId()));
        mongoTemplate.remove(deleteQuery, WorkflowDefToWorkflowDocument.class);
    }

    private WorkflowModel readWorkflow(String workflowId) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("workflow_id").is(workflowId));
        WorkflowDocument workflowDocument =
                mongoTemplate.findOne(searchQuery, WorkflowDocument.class);
        return null != workflowDocument
                ? readValue(workflowDocument.getJson_data(), WorkflowModel.class)
                : null;
    }

    private boolean insertEventExecution(EventExecution eventExecution) {

        EventExecutionDocument eventExecutionDocument = new EventExecutionDocument();
        eventExecutionDocument.setEvent_handler_name(eventExecution.getName());
        eventExecutionDocument.setEvent_name(eventExecution.getEvent());
        eventExecutionDocument.setMessage_id(eventExecution.getMessageId());
        eventExecutionDocument.setExecution_id(eventExecution.getId());
        eventExecutionDocument.setJson_data(toJson(eventExecution));
        eventExecutionDocument = mongoTemplate.save(eventExecutionDocument);
        return eventExecutionDocument != null;
    }

    public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName,
            String messageId, int max) {
        try {
            List<EventExecution> executions = Lists.newLinkedList();

            for (int i = 0; i < max; i++) {
                String executionId = messageId + "_" + i; // see SimpleEventProcessor.handle to
                // understand how the
                // execution id is set
                EventExecution ee =
                        readEventExecution(eventHandlerName, eventName, messageId, executionId);
                if (ee == null) {
                    break;
                }
                executions.add(ee);
            }

            return executions;
        } catch (Exception e) {
            String message = String.format(
                    "Unable to get event executions for eventHandlerName=%s, eventName=%s, messageId=%s",
                    eventHandlerName, eventName, messageId);
            throw new NonTransientException(message, e);
        }
    }

    private EventExecution readEventExecution(String eventHandlerName, String eventName,
            String messageId, String executionId) {

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("event_handler_name").is(eventHandlerName)
                .and("event_name").is(eventName).and("message_id").is(messageId).and("execution_id")
                .is(executionId));
        EventExecutionDocument eventExecutionDocument =
                mongoTemplate.findOne(searchQuery, EventExecutionDocument.class);

        return null != eventExecutionDocument
                ? readValue(eventExecutionDocument.getJson_data(), EventExecution.class)
                : null;
    }

    private void insertOrUpdatePollData(PollData pollData, String domain) {

        Query updateQuery = new Query();
        updateQuery.addCriteria(
                Criteria.where("queue_name").is(pollData.getQueueName()).and("domain").is(domain));

        Update update = new Update();
        update.set("json_data", toJson(pollData));
        mongoTemplate.updateMulti(updateQuery, update, PollDataDocument.class).getModifiedCount();

        long rowsUpdated = mongoTemplate.updateMulti(updateQuery, update, PollDataDocument.class)
                .getModifiedCount();

        if (rowsUpdated == 0) {
            PollDataDocument pollDataDocument = new PollDataDocument();
            pollDataDocument.setDomain(domain);
            pollDataDocument.setQueue_name(pollData.getQueueName());
            pollDataDocument.setJson_data(toJson(pollData));
            pollDataDocument = mongoTemplate.save(pollDataDocument);
        }
    }

    private PollData readPollData(String queueName, String domain) {

        Query searchQuery = new Query();
        searchQuery
                .addCriteria(Criteria.where("queue_name").is(queueName).and("domain").is(domain));

        PollDataDocument pollDataDocument =
                mongoTemplate.findOne(searchQuery, PollDataDocument.class);
        return null != pollDataDocument ? readValue(pollDataDocument.getJson_data(), PollData.class)
                : null;
    }

    private List<PollData> readAllPollData(String queueName) {

        List<PollData> pollDataList = new ArrayList<PollData>();

        Query searchQuery = new Query();
        searchQuery.addCriteria(Criteria.where("queue_name").is(queueName));

        List<PollDataDocument> pollDataDocuments =
                mongoTemplate.find(searchQuery, PollDataDocument.class);

        pollDataDocuments
                .forEach(pdd -> pollDataList.add(readValue(pdd.getJson_data(), PollData.class)));

        return pollDataList;
    }
}
