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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
 * and can only be set dynamically.
 */
public class DynamicConfig {

    public static final Set<String> ALL_DYNAMIC_CONFIGS;

    static {
        Set<String> allDynamicConfigs = new HashSet<>(SslConfigs.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(SslConfigs.DYNAMIC_LISTENER_CONFIGS);
        allDynamicConfigs.addAll(CleanerConfig.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.values());
        allDynamicConfigs.addAll(ServerConfigs.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.add(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG);
        allDynamicConfigs.add(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG);
        allDynamicConfigs.addAll(MetricConfigs.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(BrokerSecurityConfigs.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(SocketServerConfigs.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(SaslConfigs.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(TransactionLogConfig.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(RemoteLogManagerConfig.RECONFIGURABLE_CONFIGS);
        ALL_DYNAMIC_CONFIGS = Collections.unmodifiableSet(allDynamicConfigs);
    }


    public static class Broker {
        private static final ConfigDef BROKER_CONFIGS;

        static {
            ConfigDef configs = QuotaConfig.brokerQuotaConfigs();

            // Filter and define all dynamic configurations
            for (Map.Entry<String, ConfigDef.ConfigKey> entry :
                    AbstractKafkaConfig.CONFIG_DEF.configKeys().entrySet()) {
                String configName = entry.getKey();
                if (ALL_DYNAMIC_CONFIGS.contains(configName)) {
                    configs.define(entry.getValue());
                }
            }
            BROKER_CONFIGS = configs;
        }

        // In order to avoid circular reference, all DynamicBrokerConfig's variables which are initialized by 
        // `DynamicConfig.Broker` should be moved to `DynamicConfig.Broker`.
        // Otherwise, those variables of DynamicBrokerConfig will see intermediate state of `DynamicConfig.Broker`, 
        // because `brokerConfigs` is created by `DynamicBrokerConfig.AllDynamicConfigs`

        public static final Set<String> NON_DYNAMIC_PROPS;

        static {
            Set<String> props = new TreeSet<>(AbstractKafkaConfig.CONFIG_DEF.names());
            props.removeAll(BROKER_CONFIGS.names());
            NON_DYNAMIC_PROPS = Collections.unmodifiableSet(props);
        }

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return BROKER_CONFIGS.configKeys();
        }

        public static Set<String> names() {
            return BROKER_CONFIGS.names();
        }

        public static Map<String, Object> validate(Properties props) {
            return DynamicConfig.validate(BROKER_CONFIGS, props, true);
        }
    }

    public static class Client {
        private static final ConfigDef CONFIG_DEF = QuotaConfig.userAndClientQuotaConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return CONFIG_DEF.configKeys();
        }

        public static Set<String> names() {
            return CONFIG_DEF.names();
        }

        public static Map<String, Object> validate(Properties props) {
            return DynamicConfig.validate(CONFIG_DEF, props, false);
        }
    }

    public static class User {
        private static final ConfigDef USER_CONFIGS = QuotaConfig.scramMechanismsPlusUserAndClientQuotaConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return USER_CONFIGS.configKeys();
        }

        public static Set<String> names() {
            return USER_CONFIGS.names();
        }

        public static Map<String, Object> validate(Properties props) {
            return DynamicConfig.validate(USER_CONFIGS, props, false);
        }
    }

    public static class Ip {
        private static final ConfigDef IP_CONFIGS = QuotaConfig.ipConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return IP_CONFIGS.configKeys();
        }

        public static Set<String> names() {
            return IP_CONFIGS.names();
        }

        public static Map<String, Object> validate(Properties props) {
            return DynamicConfig.validate(IP_CONFIGS, props, false);
        }
    }

    public static class ClientMetrics {
        private static final ConfigDef CLIENT_CONFIGS =
                org.apache.kafka.server.metrics.ClientMetricsConfigs.configDef();

        public static Set<String> names() {
            return CLIENT_CONFIGS.names();
        }
    }

    public static class Group {
        private static final ConfigDef GROUP_CONFIGS = GroupConfig.configDef();

        public static Set<String> names() {
            return GROUP_CONFIGS.names();
        }
    }

    private static Map<String, Object> validate(ConfigDef configDef, Properties props, boolean customPropsAllowed) {
        // Validate Names
        Set<String> names = configDef.names();
        Set<String> propKeys = new HashSet<>();
        for (Object key : props.keySet()) {
            propKeys.add((String) key);
        }

        if (!customPropsAllowed) {
            Set<String> unknownKeys = new HashSet<>(propKeys);
            unknownKeys.removeAll(names);
            if (!unknownKeys.isEmpty()) {
                throw new IllegalArgumentException("Unknown Dynamic Configuration: " + unknownKeys);
            }
        }
        Properties propResolved = resolveVariableConfigs(props);
        // Validate Values
        return configDef.parse(propResolved);
    }

    private static Properties resolveVariableConfigs(Properties propsOriginal) {
        Properties props = new Properties();
        AbstractConfig config = new AbstractConfig(new ConfigDef(), propsOriginal,
                Utils.castToStringObjectMap(propsOriginal), false);
        config.originals().forEach((key, value) -> {
            if (!key.startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
                props.put(key, value);
            }
        });
        return props;
    }
}
