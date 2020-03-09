package com.example.r2dbc.example;

import junit.framework.TestCase;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.scheduling.annotation.Schedules;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;
import org.w3c.dom.css.Counter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Jitter;
import reactor.retry.Repeat;
import reactor.test.StepVerifier;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@ContextConfiguration(initializers = R2DbcExampleApplicationTests.TestDatabaseConfiguration.class)
@SpringBootTest
@Slf4j
class R2DbcExampleApplicationTests {

	private static final int MESSAGE_COUNT = 50_000;

	@Resource
	private DatabaseClient databaseClient;

	@Test
	void test() throws Exception {
		Flux<TopicMessage> generator = Flux.range(1, MESSAGE_COUNT)
				.map(i -> TopicMessage.builder().consensusTimestamp((long)i).build());

		databaseClient.insert()
				.into(TopicMessage.class)
				.using(generator)
				.fetch()
				.all()
				.blockLast();
		log.info("Row insertion complete");

		Collection<TestCase> testCases = new ArrayList<>();
		Collection<TestCase> unverified = new ArrayList<>();

		for (int i = 1; i < 4; i++) {
			testCases.add(queryAndCancel(i));
		}

		for (TestCase testCase : testCases) {
			try {
				log.info("Verifying test case: {}", testCase);
				testCase.getStepVerifier().verify();
				Assertions.assertIterableEquals(LongStream.range(1, testCase.getLimit() + 1).boxed().collect(Collectors.toList()), testCase.getValues());
			} catch (Throwable e) {
				log.warn("Error for {}: {}", testCase, e.getMessage());
				unverified.add(testCase);
			}
		}

		log.info("Unverified test cases: {}", unverified);
		Assertions.assertTrue(unverified.isEmpty());
	}

	private TestCase queryAndCancel(int id) {
		Criteria.CriteriaStep whereClause = Criteria.where("realm_num")
				.is(0)
				.and("topic_num")
				.is(0)
				.and("consensus_timestamp");

		int limit = MESSAGE_COUNT;//ThreadLocalRandom.current().nextInt(MESSAGE_COUNT);
		AtomicLong counter = new AtomicLong();
		Collection<Long> values = new ConcurrentLinkedQueue<>();

		StepVerifier stepVerifier = Flux.defer(() -> databaseClient.select()
					.from(TopicMessage.class)
					.matching(whereClause.greaterThan(counter.get()))
					.orderBy(Sort.by("consensus_timestamp"))
					.page(PageRequest.of(0, 1000))
					.fetch()
					.all()
					.doOnSubscribe(s -> log.debug("[{}] Executing query with {}/{} messages", id, counter, limit)))
				.repeatWhen(Repeat.times(Long.MAX_VALUE)
						.fixedBackoff(Duration.ofSeconds(1))
						.jitter(Jitter.random(0.2)))
				.as(t -> t.limitRequest(limit))
				.doOnSubscribe(s -> log.info("[{}] Executing query with limit {}", id, limit))
				.doOnCancel(() -> log.info("[{}] Cancelled query with {}/{} messages", id, counter.get(), limit))
				.doOnCancel(() -> {System.exit(1);})
				.doOnComplete(() -> log.info("[{}] Completed query with {}/{} messages", id, counter.get(), limit))
				.doOnNext(t -> log.trace("[{}] onNext: {}", id, t))
				.doOnNext(t -> counter.incrementAndGet())
				.doOnNext(t -> Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS)) // Simulate client backpressure
				.map(TopicMessage::getConsensusTimestamp)
				.doOnNext(values::add)
				.timeout(Duration.ofSeconds(30))
				.publishOn(Schedulers.boundedElastic())
				.subscribeOn(Schedulers.elastic())
				.as(StepVerifier::create)
				.expectNextCount(limit)
				.expectComplete()
				.verifyLater();

		return new TestCase(stepVerifier, id, limit, counter, values);
	}

	@Data
	private class TestCase {
		private final StepVerifier stepVerifier;
		private final int id;
		private final int limit;
		private final AtomicLong count;
		@ToString.Exclude
		private final Collection<Long> values;
	}

	@TestConfiguration
	static class TestDatabaseConfiguration implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		private static PostgreSQLContainer postgresql;

		@Override
		public void initialize(ConfigurableApplicationContext applicationContext) {
			try {
				postgresql = new PostgreSQLContainer<>("postgres:9.6-alpine");
				postgresql.start();

				TestPropertyValues
						.of("spring.r2dbc.name=" + postgresql.getDatabaseName())
						.and("spring.r2dbc.password=" + postgresql.getPassword())
						.and("spring.r2dbc.username=" + postgresql.getUsername())
						.and("spring.r2dbc.url=" + postgresql.getJdbcUrl()
								.replace("jdbc:", "r2dbc:"))
						.applyTo(applicationContext);
			} catch (Throwable ex) {
			}
		}

		@PreDestroy
		public void stop() {
			if (postgresql != null && postgresql.isRunning()) {
				postgresql.stop();
			}
		}
	}
}
