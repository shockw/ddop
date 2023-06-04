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

package com.reache.ddop.scheduler.plugin.task.sqoop.generator;

import com.reache.ddop.scheduler.plugin.task.sqoop.SqoopTaskExecutionContext;
import com.reache.ddop.scheduler.plugin.task.sqoop.parameter.SqoopParameters;

/**
 * Source Generator Interface
 */
public interface ISourceGenerator {

    /**
     * generate the source script
     *
     * @param sqoopParameters      sqoopParameters
     * @param sqoopTaskExecutionContext sqoopTaskExecutionContext
     * @return source script
     */
    String generate(SqoopParameters sqoopParameters, SqoopTaskExecutionContext sqoopTaskExecutionContext);
}
