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

import java.util.Properties;
import java.util.concurrent.Future;

import javax.annotation.PreDestroy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Repository;

import ufo.example.kafka.config.KafkaConfig;

@Repository
public class KafkaProducerServiceImpl implements KafkaProducerService {

	private final Producer<Long, String> producer = createProducer();

	@PreDestroy
	private void preDestroy() {
		producer.close();
	}

	@Override
	public RecordMetadata[] sendSync(ProducerRecord<Long, String>... records) {
		final RecordMetadata[] response = new RecordMetadata[records.length];

		try {
			for (int i = 0; i < records.length; i++) {

				final ProducerRecord<Long, String> record = records[i];

				// All sends are Asynchronous and return a Future. To wait synchronously
				// we must get the data from the future
				final Future<RecordMetadata> result = producer.send(record);
				final RecordMetadata metadata = result.get();
				response[i] = metadata;
			}
			return response;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		} finally {
			producer.flush();
		}
	}



	@Override
	public void sendAsync(ProducerRecord<Long, String> record, Callback callback) {
		final ProducerRecord<Long, String>[] records = new ProducerRecord[1];
		records[0] = record;
		sendAsync(records, callback);
	}

	@Override
	public void sendAsync(ProducerRecord<Long, String>[] records, Callback callback) {

		try {
			for (final ProducerRecord<Long, String> record : records) {
				producer.send(record, callback);
			}
		} catch (final Exception e) {
			throw new RuntimeException(e);
		} finally {
			producer.flush();
		}
	}

    private static Producer<Long, String> createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}
