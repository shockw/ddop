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

package com.reache.ddop.scheduler.data.quality.flow.batch.writer.file;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.reache.ddop.scheduler.data.quality.Constants.SAVE_MODE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.reache.ddop.scheduler.data.quality.config.Config;
import com.reache.ddop.scheduler.data.quality.config.ValidateResult;
import com.reache.ddop.scheduler.data.quality.execution.SparkRuntimeEnvironment;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchWriter;
import com.reache.ddop.scheduler.data.quality.utils.ConfigUtils;

/**
 * BaseFileWriter
 */
public abstract class BaseFileWriter implements BatchWriter {

    public static final String PARTITION_BY = "partition_by";
    public static final String SERIALIZER = "serializer";
    public static final String PATH = "path";

    private final Config config;

    protected BaseFileWriter(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public void prepare(SparkRuntimeEnvironment prepareEnv) {
        Map<String,Object> defaultConfig = new HashMap<>();

        defaultConfig.put(PARTITION_BY, Collections.emptyList());
        defaultConfig.put(SAVE_MODE,"error");
        defaultConfig.put(SERIALIZER,"csv");

        config.merge(defaultConfig);
    }

    protected ValidateResult checkConfigImpl(List<String> allowedUri) {

        if (Boolean.TRUE.equals(config.has(PATH)) && !Strings.isNullOrEmpty(config.getString(PATH))) {
            String dir = config.getString(PATH);
            if (dir.startsWith("/") || uriInAllowedSchema(dir, allowedUri)) {
                return new ValidateResult(true, "");
            } else {
                return new ValidateResult(false, "invalid path URI, please set the following allowed schemas: " + String.join(",", allowedUri));
            }
        } else {
            return new ValidateResult(false, "please specify [path] as non-empty string");
        }
    }

    protected boolean uriInAllowedSchema(String uri, List<String> allowedUri) {
        return allowedUri.stream().map(uri::startsWith).reduce(true, (a, b) -> a && b);
    }

    protected String buildPathWithDefaultSchema(String uri, String defaultUriSchema) {
        return uri.startsWith("/") ? defaultUriSchema + uri : uri;
    }

    protected void outputImpl(Dataset<Row> df, String defaultUriSchema) {

        DataFrameWriter<Row> writer = df.write().mode(config.getString(SAVE_MODE));

        if (CollectionUtils.isNotEmpty(config.getStringList(PARTITION_BY))) {
            List<String> partitionKeys = config.getStringList(PARTITION_BY);
            writer.partitionBy(partitionKeys.toArray(new String[]{}));
        }

        Config fileConfig = ConfigUtils.extractSubConfig(config, "options.", false);
        if (fileConfig.isNotEmpty()) {
            Map<String,String> optionMap = new HashMap<>(16);
            fileConfig.entrySet().forEach(x -> optionMap.put(x.getKey(),String.valueOf(x.getValue())));
            writer.options(optionMap);
        }

        String path = buildPathWithDefaultSchema(config.getString(PATH), defaultUriSchema);

        switch (config.getString(SERIALIZER)) {
            case "csv":
                writer.csv(path);
                break;
            case "json":
                writer.json(path);
                break;
            case "parquet":
                writer.parquet(path);
                break;
            case "text":
                writer.text(path);
                break;
            case "orc":
                writer.orc(path);
                break;
            default:
                break;
        }
    }
}
