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
package com.reache.ddop.scheduler.dao.mapper;

import com.reache.ddop.scheduler.common.constants.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import com.reache.ddop.scheduler.dao.BaseDaoTest;
import com.reache.ddop.scheduler.dao.entity.ResourcesUser;

import java.util.Date;
import java.util.List;

public class ResourceUserMapperTest extends BaseDaoTest {

    @Autowired
    private ResourceUserMapper resourceUserMapper;

    /**
     * insert
     * @return ResourcesUser
     */
    private ResourcesUser insertOne(){
        //insertOne
        ResourcesUser resourcesUser = new ResourcesUser();
        resourcesUser.setCreateTime(new Date());
        resourcesUser.setUpdateTime(new Date());
        resourcesUser.setUserId(11111);
        resourcesUser.setResourcesId(1110);
        resourcesUser.setPerm(Constants.AUTHORIZE_WRITABLE_PERM);
        resourceUserMapper.insert(resourcesUser);
        return resourcesUser;
    }

    /**
     * test update
     */
    @Test
    public void testUpdate(){
        //insertOne
        ResourcesUser queue = insertOne();
        queue.setCreateTime(new Date());
        //update
        int update = resourceUserMapper.updateById(queue);
        Assert.assertEquals(1, update);
    }

    /**
     * test delete
     */
    @Test
    public void testDelete(){
        ResourcesUser queue = insertOne();
        int delete = resourceUserMapper.deleteById(queue.getId());
        Assert.assertEquals(1, delete);
    }

    /**
     * test query
     */
    @Test
    public void testQuery() {
        ResourcesUser queue = insertOne();
        //query
        List<ResourcesUser> queues = resourceUserMapper.selectList(null);
        Assert.assertNotEquals(queues.size(), 0);
    }

    /**
     * test delete
     */
    @Test
    public void testDeleteResourceUser() {

        ResourcesUser queue = insertOne();
        int delete = resourceUserMapper.deleteResourceUser(
                queue.getUserId(),
                queue.getResourcesId());
        Assert.assertNotEquals(delete, 0);
    }

    /**
     * test delete
     */
    @Test
    public void testDeleteResourceUserArray() {

        ResourcesUser resourcesUser = insertOne();
        Integer[] resourceIdArray = new Integer[]{resourcesUser.getResourcesId()};
        int delete = resourceUserMapper.deleteResourceUserArray(
                resourcesUser.getUserId(),
                resourceIdArray);
        Assert.assertNotEquals(delete, 0);
    }
}