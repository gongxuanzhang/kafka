/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;

public class SamplePredicate implements Predicate<SourceRecord>, Versioned {

    private boolean testResult;
    boolean closed = false;

    public SamplePredicate() { }

    public SamplePredicate(boolean testResult) {
        this.testResult = testResult;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("predconfig", ConfigDef.Type.STRING, "default", ConfigDef.Importance.LOW, "docs");
    }

    @Override
    public boolean test(SourceRecord record) {
        return testResult;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void configure(Map<String, ?> configs) { }

}
