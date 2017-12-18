/*******************************************************************************
 * Copyright 2017 Francesco Cina'
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package ufo.example.kafka.config;

import javax.annotation.PostConstruct;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import kafka.utils.ZkUtils;
import ufo.example.kafka.util.KafkaTopicUtils;

@Configuration
public class KafkaConfig {

	private final Logger logger = LoggerFactory.getLogger(getClass());
    public final static String TOPIC = "my-example-topic";
    public final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private final static String ZOOKEPER_SERVERS = "127.0.0.1:2181";

    @PostConstruct
    public void createTopic() {
    	logger.debug("Creating Topic [{}]", TOPIC);
    	ZkClient zkClient = null;
    	try {
    		zkClient = KafkaTopicUtils.buildZkClient(ZOOKEPER_SERVERS);
    		final ZkUtils zkUtils = KafkaTopicUtils.buildZkUtils(zkClient, ZOOKEPER_SERVERS);
    		final int noOfPartitions = 1;
			final int noOfReplication =1;
			KafkaTopicUtils.createTopic(zkUtils, TOPIC, noOfPartitions, noOfReplication);
			logger.debug("Topic [{}] Created", TOPIC);
    	} finally {
    		if (zkClient!=null) {
    			zkClient.close();
    		}
		}
    }

}
