/*
 * Copyright 2024 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.mongo.entities;

import java.util.Date;

import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.Data;

@Document(collection = "workflow_pending")
@Data
@CompoundIndexes({
    @CompoundIndex(
            name = "workflow_pending_pk_uk",
            def = "{'workflow_type' : 1, 'workflow_id': 1}",
            unique = true)
})
public class WorkflowPendingDocument {

    @Indexed(name = "workflow_pending_workflow_type_idx")
    @Field("workflow_type")
    private String workflow_type;

    @Field("workflow_id")
    private String workflow_id;

    @Field("created_on")
    @Temporal(TemporalType.TIMESTAMP)
    private Date created_on;

    @Field("modified_on")
    @Temporal(TemporalType.TIMESTAMP)
    private Date modified_on;

    public WorkflowPendingDocument() {
        super();
        // TODO Auto-generated constructor stub
    }

    public WorkflowPendingDocument(
            String workflow_type, String workflow_id, Date created_on, Date modified_on) {
        super();
        this.workflow_type = workflow_type;
        this.workflow_id = workflow_id;
        this.created_on = created_on;
        this.modified_on = modified_on;
    }

    @PrePersist
    protected void onCreate() {
        created_on = modified_on = new Date();
    }

    @PreUpdate
    protected void onUpdate() {
        modified_on = new Date();
    }

    public String getWorkflow_type() {
        return workflow_type;
    }

    public void setWorkflow_type(String workflow_type) {
        this.workflow_type = workflow_type;
    }

    public String getWorkflow_id() {
        return workflow_id;
    }

    public void setWorkflow_id(String workflow_id) {
        this.workflow_id = workflow_id;
    }

    public Date getCreated_on() {
        return created_on;
    }

    public void setCreated_on(Date created_on) {
        this.created_on = created_on;
    }

    public Date getModified_on() {
        return modified_on;
    }

    public void setModified_on(Date modified_on) {
        this.modified_on = modified_on;
    }
}
