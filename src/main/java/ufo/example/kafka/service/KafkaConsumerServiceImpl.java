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
package ufo.example.kafka.service;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Repository;

import ufo.example.kafka.config.KafkaConfig;

@Repository
public class KafkaConsumerServiceImpl implements KafkaConsumerService {

	@Override
	public void runConsumer(String groupId, Callable<Boolean> stop, java.util.function.Consumer<ConsumerRecord<Long, String>> callback) {
		final Consumer<Long, String> consumer = KafkaConsumerServiceImpl.createConsumer(groupId);
		try {
			while (!stop.call()) {
				System.out.println("Polling for messages");
				final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
				consumerRecords.forEach(record -> {
					callback.accept(record);
				});
			}
		} catch (final Exception e) {
			throw new RuntimeException(e);
		} finally {
			consumer.commitSync();
			consumer.close();
		}
	}

	public static Consumer<Long, String> createConsumer(String groupId) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Create the consumer using props.
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC));
		return consumer;
	}
}
