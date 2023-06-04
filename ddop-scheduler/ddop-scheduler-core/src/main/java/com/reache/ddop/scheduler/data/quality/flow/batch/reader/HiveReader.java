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

package com.reache.ddop.scheduler.data.quality.flow.batch.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.reache.ddop.scheduler.data.quality.Constants.DATABASE;
import static com.reache.ddop.scheduler.data.quality.Constants.SQL;
import static com.reache.ddop.scheduler.data.quality.Constants.TABLE;

import java.util.Arrays;

import com.google.common.base.Strings;
import com.reache.ddop.scheduler.data.quality.config.Config;
import com.reache.ddop.scheduler.data.quality.config.ValidateResult;
import com.reache.ddop.scheduler.data.quality.execution.SparkRuntimeEnvironment;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchReader;

/**
 * HiveReader
 */
public class HiveReader implements BatchReader {

    private final Config config;

    public HiveReader(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public ValidateResult validateConfig() {
        return validate(Arrays.asList(DATABASE, TABLE));
    }

    @Override
    public void prepare(SparkRuntimeEnvironment prepareEnv) {
        if (Strings.isNullOrEmpty(config.getString(SQL))) {
            config.put(SQL,"select * from " + config.getString(DATABASE) + "." + config.getString(TABLE));
        }
    }

    @Override
    public Dataset<Row> read(SparkRuntimeEnvironment env) {
        return env.sparkSession().sql(config.getString(SQL));
    }

}
