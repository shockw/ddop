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

package com.reache.ddop.scheduler.dao.repository.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.reache.ddop.scheduler.dao.entity.ProcessDefinition;
import com.reache.ddop.scheduler.dao.mapper.ProcessDefinitionMapper;
import com.reache.ddop.scheduler.dao.model.PageListingResult;
import com.reache.ddop.scheduler.dao.repository.ProcessDefinitionDao;

@Repository
public class ProcessDefinitionDaoImpl implements ProcessDefinitionDao {

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Override
    public PageListingResult<ProcessDefinition> listingProcessDefinition(int pageNumber, int pageSize, String searchVal,
                                                                         int userId, long projectCode) {
        Page<ProcessDefinition> page = new Page<>(pageNumber, pageSize);
        IPage<ProcessDefinition> processDefinitions =
                processDefinitionMapper.queryDefineListPaging(page, searchVal, userId, projectCode);

        return PageListingResult.<ProcessDefinition>builder()
                .totalCount(processDefinitions.getTotal())
                .currentPage(pageNumber)
                .pageSize(pageSize)
                .records(processDefinitions.getRecords())
                .build();
    }
}
