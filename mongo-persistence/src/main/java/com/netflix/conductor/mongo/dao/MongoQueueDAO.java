/*
 * Copyright 2024 Conductor Authors. <p> Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at <p> http://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.mongo.dao;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.mongo.entities.QueueDocument;
import com.netflix.conductor.mongo.entities.QueueMessageDocument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;

@Trace
public class MongoQueueDAO extends MongoBaseDAO implements QueueDAO {

    private static final Long UNACK_SCHEDULE_MS = 60_000L;

    private final MongoTemplate mongoTemplate;

    public MongoQueueDAO(ObjectMapper objectMapper, MongoTemplate mongoTemplate) {
        super(objectMapper);
        this.mongoTemplate = mongoTemplate;

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::processAllUnacks,
                UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
        logger.debug(MongoQueueDAO.class.getName() + " is ready to serve");
    }

    /*
     * ---------------------------------------------------------------------------------------
     * QueueDAO Interface
     * ---------------------------------------------------------------------------------------
     */

    /**
     * @param queueName name of the queue
     * @param id message id
     * @param offsetTimeInSecond time in seconds, after which the message should be marked visible.
     *        (for timed queues)
     */
    @Override
    public void push(String queueName, String id, long offsetTimeInSecond) {
        push(queueName, id, 0, offsetTimeInSecond);
    }

    /**
     * @param queueName name of the queue
     * @param id message id
     * @param priority message priority (between 0 and 99)
     * @param offsetTimeInSecond time in seconds, after which the message should be marked visible.
     *        (for timed queues)
     */
    @Override
    public void push(String queueName, String id, int priority, long offsetTimeInSecond) {
        pushMessage(queueName, id, null, priority, offsetTimeInSecond);
    }

    /**
     * @param queueName Name of the queue
     * @param messages messages to be pushed.
     */
    @Override
    public void push(String queueName, List<Message> messages) {
        messages.forEach(message -> pushMessage(queueName, message.getId(), message.getPayload(),
                message.getPriority(), 0));
    }

    /**
     * @param queueName Name of the queue
     * @param id message id
     * @param offsetTimeInSecond time in seconds, after which the message should be marked visible
     *        (for timed queues)
     * @return true if the element was added to the queue. false otherwise indicating the element
     *         already exists in the queue.
     */
    @Override
    public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
        return pushIfNotExists(queueName, id, 0, offsetTimeInSecond);
    }

    /**
     * @param queueName Name of the queue
     * @param id message id
     * @param priority message priority (between 0 and 99)
     * @param offsetTimeInSecond time in seconds, after which the message should be marked visible
     *        (for timed queues)
     * @return true if the element was added to the queue. false otherwise indicating the element
     *         already exists in the queue.
     */
    @Override
    public boolean pushIfNotExists(String queueName, String id, int priority,
            long offsetTimeInSecond) {
        if (!existsMessage(queueName, id)) {
            pushMessage(queueName, id, null, priority, offsetTimeInSecond);
            return true;
        }
        return false;
    }

    /**
     * @param queueName Name of the queue
     * @param count number of messages to be read from the queue
     * @param timeout timeout in milliseconds
     * @return list of elements from the named queue
     */
    @Override
    public List<String> pop(String queueName, int count, int timeout) {
        List<Message> messages = popMessages(queueName, count, timeout);
        if (messages == null) {
            return new ArrayList<>();
        }
        return messages.stream().map(Message::getId).collect(Collectors.toList());
    }

    /**
     * @param queueName Name of the queue
     * @param count number of messages to be read from the queue
     * @param timeout timeout in milliseconds
     * @return list of elements from the named queue
     */
    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        List<Message> messages = popMessages(queueName, count, timeout);
        if (messages == null) {
            return new ArrayList<>();
        }
        return messages;
    }

    /**
     * @param queueName Name of the queue
     * @param messageId Message id
     */
    @Override
    public void remove(String queueName, String messageId) {
        removeMessage(queueName, messageId);
    }

    /**
     * @param queueName Name of the queue
     * @return size of the queue
     */
    @Override
    public int getSize(String queueName) {
        return ((Long) mongoTemplate.count(
                new Query().addCriteria(Criteria.where("queue_name").is(queueName)),
                QueueMessageDocument.class)).intValue();
    }

    /**
     * @param queueName Name of the queue
     * @param messageId Message Id
     * @return true if the message was found and ack'ed
     */
    @Override
    public boolean ack(String queueName, String messageId) {
        return removeMessage(queueName, messageId);
    }

    /**
     * Extend the lease of the unacknowledged message for longer period.
     *
     * @param queueName Name of the queue
     * @param messageId Message Id
     * @param unackTimeout timeout in milliseconds for which the unack lease should be extended
     *        (replaces the current value with this value)
     * @return true if the message was updated with extended lease. false otherwise.
     */
    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        long updatedOffsetTimeInSecond = unackTimeout / 1000;

        Query query = new Query();
        query.addCriteria(
                Criteria.where("queue_name").is(queueName).and("message_id").is(messageId));
        Update update = new Update().max(DAO_NAME, query);
        update.set("offset_time_seconds", updatedOffsetTimeInSecond);
        update.set("deliver_on", getOffsetAddedDate(((Long) updatedOffsetTimeInSecond).intValue()));

        return mongoTemplate.updateMulti(query, update, QueueMessageDocument.class)
                .getModifiedCount() == 1;
    }

    /**
     * @param queueName Name of the queue
     */
    @Override
    public void flush(String queueName) {
        mongoTemplate.remove(new Query().addCriteria(Criteria.where("queue_name").is(queueName)),
                QueueMessageDocument.class);
    }

    /**
     * @return key : queue name, value: size of the queue
     */
    @Override
    public Map<String, Long> queuesDetail() {
        Map<String, Long> detail = Maps.newHashMap();
        Query findQuery = new Query();
        findQuery.addCriteria(Criteria.where("popped").is(false));

        List<String> queueNames = mongoTemplate.findDistinct(findQuery, "queue_name",
                QueueMessageDocument.class, String.class);

        queueNames.forEach(queueName -> {
            Query query = new Query();
            query.addCriteria(Criteria.where("popped").is(false).and("queue_name").is(queueName));
            long size = mongoTemplate.count(query, QueueMessageDocument.class);
            detail.put(queueName, size);
        });

        return detail;
    }

    /**
     * @return key : queue name, value: map of shard name to size and unack queue size
     */
    @Override
    public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
        Map<String, Map<String, Map<String, Long>>> result = Maps.newHashMap();

        List<String> queueNames = mongoTemplate.findDistinct(new Query(), "queue_name",
                QueueMessageDocument.class, String.class);

        queueNames.forEach(queueName -> {
            Query sizeQuery = new Query();
            sizeQuery.addCriteria(
                    Criteria.where("popped").is(false).and("queue_name").is(queueName));
            long size = mongoTemplate.count(sizeQuery, QueueMessageDocument.class);

            Query unackedQuery = new Query();
            unackedQuery
                    .addCriteria(Criteria.where("popped").is(true).and("queue_name").is(queueName));
            long queueUnacked = mongoTemplate.count(unackedQuery, QueueMessageDocument.class);

            result.put(queueName, ImmutableMap.of("a", ImmutableMap.of( // sharding not implemented,
                                                                        // returning only
                    // one shard with all the info
                    "size", size, "uacked", queueUnacked)));
        });

        return result;
    }

    /**
     * This method is not implemented for MongoDB backed Conductor
     *
     * <p>
     * Process un-acks.
     *
     * @param queueName Name of the queue
     */
    // Refer to the default implementation in QueueDAO
    // @Override
    // public void processUnacks(String queueName) {}

    /**
     * Resets the offsetTime on a message to 0, without pulling out the message from the queue
     *
     * @param queueName name of the queue
     * @param id message id
     * @return true if the message is in queue and the change was successful else returns false
     */
    @Override
    public boolean resetOffsetTime(String queueName, String id) {
        QueueMessageDocument aQueueMessageDocument = mongoTemplate.findOne(
                new Query().addCriteria(
                        Criteria.where("queue_name").is(queueName).and("message_id").is(id)),
                QueueMessageDocument.class);
        if (null == aQueueMessageDocument)
            return false;
        else {
            aQueueMessageDocument.setOffsetTimeSeconds(0);
            aQueueMessageDocument.setDeliverOn(getOffsetAddedDate(0));
            return mongoTemplate.save(aQueueMessageDocument) != null;
        }
    }

    /**
     * Postpone a given message with postponeDurationInSeconds, so that the message won't be
     * available for further polls until specified duration. By default, the message is removed and
     * pushed backed with postponeDurationInSeconds to be backwards compatible.
     *
     * @param queueName name of the queue
     * @param messageId message id
     * @param priority message priority (between 0 and 99)
     * @param postponeDurationInSeconds duration in seconds by which the message is to be postponed
     */
    // Refer to the default implementation in QueueDAO
    // @Override
    // public default boolean postpone(String queueName, String messageId, int
    // priority, long postponeDurationInSeconds) {}

    /**
     * Check if the message with given messageId exists in the Queue.
     *
     * @param queueName
     * @param messageId
     * @return
     */
    @Override
    public boolean containsMessage(String queueName, String messageId) {
        Query searchQuery = new Query();
        searchQuery.addCriteria(
                Criteria.where("queue_name").is(queueName).and("message_id").is(messageId));

        return mongoTemplate.exists(searchQuery, QueueMessageDocument.class);
    }

    /*
     * ---------------------------------------------------------------------------------------
     * Implementation
     * ---------------------------------------------------------------------------------------
     */

    private void pushMessage(String queueName, String messageId, String payload, Integer priority,
            long offsetTimeInSecond) {

        createQueueIfNotExists(queueName);

        QueueMessageDocument aQueueMessageDocument = mongoTemplate.findOne(
                new Query().addCriteria(
                        Criteria.where("queue_name").is(queueName).and("message_id").is(messageId)),
                QueueMessageDocument.class);

        if (null != aQueueMessageDocument) {
            aQueueMessageDocument.setPriority(priority);
            aQueueMessageDocument.setOffsetTimeSeconds(offsetTimeInSecond);
            aQueueMessageDocument
                    .setDeliverOn(getOffsetAddedDate(((Long) offsetTimeInSecond).intValue()));
            aQueueMessageDocument.setPopped(false);
            aQueueMessageDocument = mongoTemplate.save(aQueueMessageDocument);
        } else {
            QueueMessageDocument newQueueMessageDocument = new QueueMessageDocument();
            newQueueMessageDocument.setQueueName(queueName);
            newQueueMessageDocument.setMessageId(messageId);
            newQueueMessageDocument.setPriority(priority);
            newQueueMessageDocument.setOffsetTimeSeconds(offsetTimeInSecond);
            newQueueMessageDocument
                    .setDeliverOn(getOffsetAddedDate(((Long) offsetTimeInSecond).intValue()));
            newQueueMessageDocument.setPopped(false);
            newQueueMessageDocument.setPayload(payload);
            newQueueMessageDocument = mongoTemplate.save(newQueueMessageDocument);
        }
    }

    private void createQueueIfNotExists(String queueName) {
        logger.trace("Creating new queue '{}'", queueName);

        Query searchQuery = new Query();

        searchQuery.addCriteria(Criteria.where("name").is(queueName));

        boolean exists = mongoTemplate.count(searchQuery, QueueDocument.class) > 0;
        if (!exists) {
            QueueDocument newQueueDocument = new QueueDocument();
            newQueueDocument.setQueueName(queueName);
            newQueueDocument = mongoTemplate.save(newQueueDocument);
        }
    }

    public void processAllUnacks() {

        logger.trace("processAllUnacks started");

        Query query = new Query();
        query.addCriteria(
                Criteria.where("popped").is(true).and("deliver_on").lt(getOffsetAddedDate(-60)));
        Update update = new Update().max(DAO_NAME, query);
        update.set("popped", false);

        mongoTemplate.updateMulti(query, update, QueueMessageDocument.class);
    }

    private boolean existsMessage(String queueName, String messageId) {

        return mongoTemplate.exists(
                new Query().addCriteria(
                        Criteria.where("queue_name").is(queueName).and("message_id").is(messageId)),
                QueueMessageDocument.class);
    }

    private List<Message> popMessages(String queueName, int count, int timeout) {
        long start = System.currentTimeMillis();
        List<Message> messages = peekMessages(queueName, count);

        while (messages.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            messages = peekMessages(queueName, count);
        }

        if (messages.isEmpty()) {
            return messages;
        }

        List<Message> poppedMessages = new ArrayList<>();
        for (Message message : messages) {

            Query query = new Query();
            query.addCriteria(Criteria.where("popped").is(false).and("queue_name").is(queueName)
                    .and("message_id").is(message.getId()));
            Update update = new Update().max(DAO_NAME, query);
            update.set("popped", true);

            if (mongoTemplate.updateMulti(query, update, QueueMessageDocument.class)
                    .getModifiedCount() == 1) {
                poppedMessages.add(message);
            }
        }
        return poppedMessages;
    }

    private List<Message> peekMessages(String queueName, int count) {
        if (count < 1) {
            return Collections.emptyList();
        }

        List<Order> orderBy = new ArrayList<Order>();

        Query query = new Query();
        query.addCriteria(Criteria.where("queue_name").is(queueName).and("popped").is(false)
                .and("deliver_on").lte(getOffsetAddedDate(1000)));
        orderBy.add(new Order(Direction.DESC, "priority"));
        orderBy.add(new Order(Direction.ASC, "created_on"));
        orderBy.add(new Order(Direction.ASC, "deliver_on"));
        query.with(Sort.by(orderBy));
        query.limit(count);

        List<Message> results = new ArrayList<>();

        List<QueueMessageDocument> aQueueMessageDocumentList =
                mongoTemplate.find(query, QueueMessageDocument.class);
        if (!aQueueMessageDocumentList.isEmpty())
            aQueueMessageDocumentList.forEach(qmd -> {
                Message m = new Message();
                m.setId(qmd.getMessageId());
                m.setPriority(qmd.getPriority());
                m.setPayload(qmd.getPayload());
                results.add(m);
            });

        return results;
    }

    private boolean removeMessage(String queueName, String messageId) {
        QueueMessageDocument queueMessageDocument = mongoTemplate.findOne(
                new Query().addCriteria(
                        Criteria.where("queue_name").is(queueName).and("message_id").is(messageId)),
                QueueMessageDocument.class);

        if (null != queueMessageDocument) {
            mongoTemplate.remove(queueMessageDocument);
            return true;
        }
        return false;
    }

    private Date getOffsetAddedDate(int offsetInSeconds) {
        Date oldDate = new Date();
        Calendar gcal = new GregorianCalendar();
        gcal.setTime(oldDate);
        gcal.add(Calendar.SECOND, offsetInSeconds);
        Date newDate = gcal.getTime();
        return newDate;
    }
}
