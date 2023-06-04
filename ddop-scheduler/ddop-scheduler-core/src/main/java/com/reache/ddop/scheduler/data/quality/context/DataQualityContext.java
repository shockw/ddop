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

package com.reache.ddop.scheduler.data.quality.context;

import java.util.List;

import com.reache.ddop.scheduler.data.quality.config.DataQualityConfiguration;
import com.reache.ddop.scheduler.data.quality.exception.DataQualityException;
import com.reache.ddop.scheduler.data.quality.execution.SparkRuntimeEnvironment;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchReader;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchTransformer;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchWriter;
import com.reache.ddop.scheduler.data.quality.flow.batch.reader.ReaderFactory;
import com.reache.ddop.scheduler.data.quality.flow.batch.transformer.TransformerFactory;
import com.reache.ddop.scheduler.data.quality.flow.batch.writer.WriterFactory;

/**
 * DataQualityContext
 */
public class DataQualityContext {

    private SparkRuntimeEnvironment sparkRuntimeEnvironment;

    private DataQualityConfiguration dataQualityConfiguration;

    public DataQualityContext() {
    }

    public DataQualityContext(SparkRuntimeEnvironment sparkRuntimeEnvironment,
                              DataQualityConfiguration dataQualityConfiguration) {
        this.sparkRuntimeEnvironment = sparkRuntimeEnvironment;
        this.dataQualityConfiguration = dataQualityConfiguration;
    }

    public void execute() throws DataQualityException {
        List<BatchReader> readers = ReaderFactory
                .getInstance()
                .getReaders(this.sparkRuntimeEnvironment,dataQualityConfiguration.getReaderConfigs());
        List<BatchTransformer> transformers = TransformerFactory
                .getInstance()
                .getTransformer(this.sparkRuntimeEnvironment,dataQualityConfiguration.getTransformerConfigs());
        List<BatchWriter> writers = WriterFactory
                .getInstance()
                .getWriters(this.sparkRuntimeEnvironment,dataQualityConfiguration.getWriterConfigs());

        if (sparkRuntimeEnvironment.isBatch()) {
            sparkRuntimeEnvironment.getBatchExecution().execute(readers,transformers,writers);
        } else {
            throw new DataQualityException("stream mode is not supported now");
        }
    }
}
