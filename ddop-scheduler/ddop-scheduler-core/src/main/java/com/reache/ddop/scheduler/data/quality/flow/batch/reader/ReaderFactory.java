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

import java.util.ArrayList;
import java.util.List;

import com.reache.ddop.scheduler.data.quality.config.Config;
import com.reache.ddop.scheduler.data.quality.config.ReaderConfig;
import com.reache.ddop.scheduler.data.quality.enums.ReaderType;
import com.reache.ddop.scheduler.data.quality.exception.DataQualityException;
import com.reache.ddop.scheduler.data.quality.execution.SparkRuntimeEnvironment;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchReader;

/**
 * ReaderFactory
 */
public class ReaderFactory {

    private static class Singleton {
        static ReaderFactory instance = new ReaderFactory();
    }

    public static ReaderFactory getInstance() {
        return Singleton.instance;
    }

    public List<BatchReader> getReaders(SparkRuntimeEnvironment sparkRuntimeEnvironment, List<ReaderConfig> readerConfigs) throws DataQualityException {

        List<BatchReader> readerList = new ArrayList<>();

        for (ReaderConfig readerConfig : readerConfigs) {
            BatchReader reader = getReader(readerConfig);
            if (reader != null) {
                reader.validateConfig();
                reader.prepare(sparkRuntimeEnvironment);
                readerList.add(reader);
            }
        }

        return readerList;
    }

    private BatchReader getReader(ReaderConfig readerConfig) throws DataQualityException {
        ReaderType readerType = ReaderType.getType(readerConfig.getType());
        Config config = new Config(readerConfig.getConfig());
        if (readerType != null) {
            switch (readerType) {
                case JDBC:
                    return new JdbcReader(config);
                case HIVE:
                    return new HiveReader(config);
                default:
                    throw new DataQualityException("reader type " + readerType + " is not supported!");
            }
        }

        return null;
    }

}
