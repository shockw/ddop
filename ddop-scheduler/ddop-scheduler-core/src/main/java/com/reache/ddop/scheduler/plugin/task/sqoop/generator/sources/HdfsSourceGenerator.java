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

package com.reache.ddop.scheduler.plugin.task.sqoop.generator.sources;

import com.reache.ddop.scheduler.common.utils.JSONUtils;

import static com.reache.ddop.scheduler.plugin.task.api.TaskConstants.SPACE;
import static com.reache.ddop.scheduler.plugin.task.sqoop.SqoopConstants.HDFS_EXPORT_DIR;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reache.ddop.scheduler.plugin.task.sqoop.SqoopTaskExecutionContext;
import com.reache.ddop.scheduler.plugin.task.sqoop.generator.ISourceGenerator;
import com.reache.ddop.scheduler.plugin.task.sqoop.parameter.SqoopParameters;
import com.reache.ddop.scheduler.plugin.task.sqoop.parameter.sources.SourceHdfsParameter;

/**
 * hdfs source generator
 */
public class HdfsSourceGenerator implements ISourceGenerator {

    private static final Logger logger = LoggerFactory.getLogger(HdfsSourceGenerator.class);

    @Override
    public String generate(SqoopParameters sqoopParameters, SqoopTaskExecutionContext sqoopTaskExecutionContext) {

        StringBuilder hdfsSourceSb = new StringBuilder();

        try {
            SourceHdfsParameter sourceHdfsParameter
                = JSONUtils.parseObject(sqoopParameters.getSourceParams(), SourceHdfsParameter.class);

            if (null != sourceHdfsParameter) {
                if (StringUtils.isNotEmpty(sourceHdfsParameter.getExportDir())) {
                    hdfsSourceSb.append(SPACE).append(HDFS_EXPORT_DIR)
                        .append(SPACE).append(sourceHdfsParameter.getExportDir());
                } else {
                    throw new IllegalArgumentException("Sqoop hdfs export dir is null");
                }

            }
        } catch (Exception e) {
            logger.error(String.format("Sqoop hdfs source parmas build failed: [%s]", e.getMessage()));
        }

        return hdfsSourceSb.toString();
    }
}
