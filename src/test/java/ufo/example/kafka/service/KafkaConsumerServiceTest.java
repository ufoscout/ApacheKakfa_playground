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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import ufo.example.kafka.DemoApplicationTests;
import ufo.example.kafka.config.KafkaConfig;

public class KafkaConsumerServiceTest extends DemoApplicationTests {

	@Autowired
	private KafkaProducerService producer;
	@Autowired
	private KafkaConsumerService consumerService;

	@Test
	public void moreConsumerSameGroup() throws InterruptedException {
		final int howMany = 15;

		final CountDownLatch countDown = new CountDownLatch(howMany);
		final AtomicBoolean done = new AtomicBoolean(false);

		final AtomicInteger messagesReceived = new AtomicInteger(0);

		final String groupId = UUID.randomUUID().toString();

		new Thread(() -> {
			consumerService.runConsumer(groupId, () -> {return done.get();}, record -> {
				messagesReceived.getAndIncrement();
				countDown.countDown();
				System.out.printf("Consumer 1 Record:(%s, %d, %s, %d, %d)\n",
						groupId, record.key(), record.value(),
						record.partition(), record.offset());
			});
		}).start();

		new Thread(() -> {
			consumerService.runConsumer(groupId, () -> {return done.get();}, record -> {
				messagesReceived.getAndIncrement();
				countDown.countDown();
				System.out.printf("Consumer 2 Record:(%s, %d, %s, %d, %d)\n",
						groupId, record.key(), record.value(),
						record.partition(), record.offset());
			});
		}).start();

		Thread.sleep(2500);

		send(howMany);

		countDown.await(5, TimeUnit.SECONDS);
		done.set(true);

		assertEquals(howMany, messagesReceived.get());
	}


	@Test
	public void moreConsumerDifferentGroup() throws InterruptedException {
		final int howMany = 15;

		final CountDownLatch countDown = new CountDownLatch(howMany * 2);
		final AtomicBoolean done = new AtomicBoolean(false);

		final AtomicInteger messagesReceived = new AtomicInteger(0);

		new Thread(() -> {
			final String groupId = UUID.randomUUID().toString();
			consumerService.runConsumer(groupId, () -> {return done.get();}, record -> {
				messagesReceived.getAndIncrement();
				countDown.countDown();
				System.out.printf("Consumer 1 Record:(%s, %d, %s, %d, %d)\n",
						groupId, record.key(), record.value(),
						record.partition(), record.offset());
			});
		}).start();

		new Thread(() -> {
			final String groupId = UUID.randomUUID().toString();
			consumerService.runConsumer(groupId, () -> {return done.get();}, record -> {
				messagesReceived.getAndIncrement();
				countDown.countDown();
				System.out.printf("Consumer 2 Record:(%s, %d, %s, %d, %d)\n",
						groupId, record.key(), record.value(),
						record.partition(), record.offset());
			});
		}).start();

		Thread.sleep(2500);

		send(howMany);

		countDown.await(5, TimeUnit.SECONDS);
		done.set(true);

		assertEquals(howMany * 2, messagesReceived.get());
	}


	private void send(int howMany) {
		System.out.println("Sending Messages");
		final long time = System.currentTimeMillis();
		final ProducerRecord<Long, String>[] requests = new ProducerRecord[howMany];

		for (int i=0; i<howMany; i++) {
			final long index = time + i;
			requests[i] = new ProducerRecord<>(KafkaConfig.TOPIC, index, "Hello Mom " + index);
		}

		producer.sendAsync(requests, (m, e) -> {});

	}

}
