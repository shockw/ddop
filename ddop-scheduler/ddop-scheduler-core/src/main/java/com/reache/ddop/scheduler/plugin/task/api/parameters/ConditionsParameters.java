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

package com.reache.ddop.scheduler.plugin.task.api.parameters;

import java.util.ArrayList;
import java.util.List;

import com.reache.ddop.scheduler.plugin.task.api.enums.DependentRelation;
import com.reache.ddop.scheduler.plugin.task.api.model.DependentTaskModel;
import com.reache.ddop.scheduler.plugin.task.api.model.ResourceInfo;

public class ConditionsParameters extends AbstractParameters {

    //depend node list and state, only need task name
    private List<DependentTaskModel> dependTaskList;
    private DependentRelation dependRelation;

    // node list to run when success
    private List<String> successNode;

    // node list to run when failed
    private List<String> failedNode;

    @Override
    public boolean checkParameters() {
        return true;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return new ArrayList<>();
    }

    public List<DependentTaskModel> getDependTaskList() {
        return dependTaskList;
    }

    public void setDependTaskList(List<DependentTaskModel> dependTaskList) {
        this.dependTaskList = dependTaskList;
    }

    public DependentRelation getDependRelation() {
        return dependRelation;
    }

    public void setDependRelation(DependentRelation dependRelation) {
        this.dependRelation = dependRelation;
    }

    public List<String> getSuccessNode() {
        return successNode;
    }

    public void setSuccessNode(List<String> successNode) {
        this.successNode = successNode;
    }

    public List<String> getFailedNode() {
        return failedNode;
    }

    public void setFailedNode(List<String> failedNode) {
        this.failedNode = failedNode;
    }

    public String getConditionResult() {
        return "{"
            + "\"successNode\": [\"" + successNode.get(0)
            + "\"],\"failedNode\": [\"" + failedNode.get(0)
            + "\"]}";
    }

}
