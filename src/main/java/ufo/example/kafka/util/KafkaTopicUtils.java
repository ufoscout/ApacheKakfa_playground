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

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaTopicUtils {

	/**
	 * Creates a Topic dynamically instead that by using the "kafka/bin/kafka-topics.sh --create" command line command and return true.
	 * If the topic already exists it will return false.
	 * The big drawback of this method is that it requires the full Kafka (plus Scala) dependencies because this it is not possible
	 * to do it with only the client.
	 *
	 * @param topicName
	 * @param noOfPartitions
	 * @param noOfReplication
	 * @throws Exception
	 */
	public static boolean createTopic(ZkUtils zkUtils, String topicName, int noOfPartitions, int noOfReplication) {
		if (!topicExists(zkUtils, topicName)) {
			final Properties topicConfiguration = new Properties();
			final RackAwareMode rackAwareMode = RackAwareMode.Safe$.MODULE$;
			AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration, rackAwareMode);
			return true;
		}
		return false;
	}

	/**
	 * check whether a topic already exists
	 * @param zkUtils
	 * @param topicName
	 * @return
	 */
	public static boolean topicExists(ZkUtils zkUtils, String topicName) {
		return AdminUtils.topicExists(zkUtils, topicName);
	}

	/**
	 * Deletes a topic
	 * @param zkUtils
	 * @param topicName
	 * @return
	 */
	public static void deleteTopic(ZkUtils zkUtils, String topicName) {
		AdminUtils.deleteTopic(zkUtils, topicName);
	}

	public static ZkClient buildZkClient(String zookeeperHosts) {
		final int sessionTimeOutInMs = 15 * 1000; // 15 secs
		final int connectionTimeOutInMs = 10 * 1000; // 10 secs
		return new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
	}

	public static ZkUtils buildZkUtils(ZkClient zkClient, String zookeeperHosts) {
		return new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);
	}

}