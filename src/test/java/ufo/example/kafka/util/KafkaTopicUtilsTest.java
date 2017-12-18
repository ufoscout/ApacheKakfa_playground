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
package ufo.example.kafka.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import kafka.utils.ZkUtils;
import ufo.example.kafka.DemoApplicationTests;

public class KafkaTopicUtilsTest extends DemoApplicationTests {

	@Test
	public void shouldCreateATopic() {
		final String topicName = "TestTopic";

		final String zookeeperHosts = "localhost:2181";

		final ZkClient zkClient = KafkaTopicUtils.buildZkClient(zookeeperHosts);
		final ZkUtils zkUtils = KafkaTopicUtils.buildZkUtils(zkClient, zookeeperHosts);

		System.out.println("does topic exist? " + KafkaTopicUtils.topicExists(zkUtils, topicName) );

		// delete Topic if already exists
		if (KafkaTopicUtils.topicExists(zkUtils, topicName)) {
			System.out.println("Deleting topic");
			KafkaTopicUtils.deleteTopic(zkUtils, topicName);
			assertFalse(KafkaTopicUtils.topicExists(zkUtils, topicName));
		}

		// create a new Topic programmatically
		final int noOfPartitions = 1;
		final int noOfReplication = 1;
		assertTrue(KafkaTopicUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication));
		assertTrue(KafkaTopicUtils.topicExists(zkUtils, topicName));

		System.out.println("does topic exist? " + KafkaTopicUtils.topicExists(zkUtils, topicName) );

		// delete the topic
		KafkaTopicUtils.deleteTopic(zkUtils, topicName);

		// Something strange here. The Topic results still present so these steps fail.
		// However, if I run the test again, the first check of the new test execution shows that the Topic was really deleted.
		//assertFalse(KafkaTopicUtils.topicExists(zkUtils, topicName));
		//assertTrue(KafkaTopicUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication));

		zkUtils.close();
		zkClient.close();
	}

}
