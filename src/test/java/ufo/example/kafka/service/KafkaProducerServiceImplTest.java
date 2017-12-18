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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import ufo.example.kafka.DemoApplicationTests;
import ufo.example.kafka.config.KafkaConfig;

public class KafkaProducerServiceImplTest extends DemoApplicationTests {

	@Autowired
	private KafkaProducerService producer;

	@Test
	public void testSendSync() {
		final long time = System.currentTimeMillis();

		final long index = System.nanoTime();
		final ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfig.TOPIC, index, "Hello Mom " + index);

		final RecordMetadata[] response = producer.sendSync(record);

		assertNotNull(response);
		assertEquals(1, response.length);

		final long elapsedTime = System.currentTimeMillis() - time;
		final RecordMetadata metadata = response[0];
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
				record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
	}

	@Test
	public void testSendAsync() throws InterruptedException {
		final long time = System.currentTimeMillis();
	    final CountDownLatch countDownLatch = new CountDownLatch(1);

		final long index = System.nanoTime();
		final ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfig.TOPIC, index, "Hello Mom " + index);

		producer.sendAsync(record, (RecordMetadata metadata, Exception exception) -> {
            final long elapsedTime = System.currentTimeMillis() - time;
            if (metadata != null) {
                System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
            } else {
                exception.printStackTrace();
            }
            countDownLatch.countDown();
        });

		countDownLatch.await();

//		assertNotNull(response);
//		assertEquals(1, response.length);
//
//		final long elapsedTime = System.currentTimeMillis() - time;
//		final RecordMetadata metadata = response[0];
//		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
	}

}
