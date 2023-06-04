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

package com.reache.ddop.scheduler.data.quality.flow.batch.transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.reache.ddop.scheduler.data.quality.config.Config;
import com.reache.ddop.scheduler.data.quality.config.ValidateResult;
import com.reache.ddop.scheduler.data.quality.execution.SparkRuntimeEnvironment;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchTransformer;

import static com.reache.ddop.scheduler.data.quality.Constants.SQL;

import java.util.Collections;

/**
 * SqlTransformer
 */
public class SqlTransformer implements BatchTransformer {

    private final Config config;

    public SqlTransformer(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public ValidateResult validateConfig() {
        return validate(Collections.singletonList(SQL));
    }

    @Override
    public void prepare(SparkRuntimeEnvironment prepareEnv) {
        // Do nothing
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> data, SparkRuntimeEnvironment env) {
        return env.sparkSession().sql(config.getString(SQL));
    }
}
