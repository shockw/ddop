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

import java.util.ArrayList;
import java.util.List;

import com.reache.ddop.scheduler.data.quality.config.Config;
import com.reache.ddop.scheduler.data.quality.config.TransformerConfig;
import com.reache.ddop.scheduler.data.quality.enums.TransformerType;
import com.reache.ddop.scheduler.data.quality.exception.DataQualityException;
import com.reache.ddop.scheduler.data.quality.execution.SparkRuntimeEnvironment;
import com.reache.ddop.scheduler.data.quality.flow.batch.BatchTransformer;

/**
 * WriterFactory
 */
public class TransformerFactory {

    private static class Singleton {
        static TransformerFactory instance = new TransformerFactory();
    }

    public static TransformerFactory getInstance() {
        return Singleton.instance;
    }

    public List<BatchTransformer> getTransformer(SparkRuntimeEnvironment sparkRuntimeEnvironment, List<TransformerConfig> transformerConfigs) throws DataQualityException {

        List<BatchTransformer> transformers = new ArrayList<>();

        for (TransformerConfig transformerConfig:transformerConfigs) {
            BatchTransformer transformer = getTransformer(transformerConfig);
            if (transformer != null) {
                transformer.validateConfig();
                transformer.prepare(sparkRuntimeEnvironment);
                transformers.add(transformer);
            }
        }

        return transformers;
    }

    private BatchTransformer getTransformer(TransformerConfig transformerConfig) throws DataQualityException {
        TransformerType transformerType = TransformerType.getType(transformerConfig.getType());
        Config config = new Config(transformerConfig.getConfig());
        if (transformerType != null) {
            if (transformerType == TransformerType.SQL) {
                return new SqlTransformer(config);
            }
            throw new DataQualityException("transformer type " + transformerType + " is not supported!");
        }

        return null;
    }

}
