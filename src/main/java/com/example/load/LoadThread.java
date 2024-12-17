package com.example.load;

import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.codec.SerializableTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;


public class LoadThread implements Runnable {
	private static final JsonObject EMPTY_JSON_OBJECT = JsonObject.create();
	long endTime;
	long runSeconds;
	int nRequestsPerSecond;
	long timeoutUs;
	long thresholdUs;
	String[] keys;
	CountDownLatch latch;
	Semaphore rateSemaphore;
	long[] baseTime;
	boolean logMax;
	boolean logTimeout;
	boolean logThreshold;
	boolean asObject;
	boolean kvGet;
	boolean kvInsert;
	int messageSize;
	Execution execution;
	int batchSize;
	boolean countMaxInParallel;

	int count = 0;
	long sum = 0;
	byte[] message;
	Recording maxRecording;

	Cluster cluster;
	String bucketName;
	Collection collection;
	boolean sameId;

	public HashMap<String, List<Recording>> recordings = new HashMap<>();

	public static AtomicBoolean first = new AtomicBoolean(true); // just print message length one time

	public long getCount() {
		return count;
	}

	public void setRunSeconds(long runSeconds) {
		this.runSeconds = runSeconds;
	}

	public void setRateSemaphore(Semaphore rateSemaphore) {
		this.rateSemaphore = rateSemaphore;
	}

	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
	}

	public void setLogMax(boolean logMax) {
		this.logMax = logMax;
	}

	public void setLogThreshold(boolean logThreshold) {
		this.logThreshold = logThreshold;
	}

	public void setLogTimeout(boolean logTimeout) {
		this.logTimeout = logTimeout;
	}

	public List<Recording> getRecordings(String key) {
		return recordings.get(key);
	}

	public Recording getRecording(String key) {
		List<Recording> l = recordings.get(key);
		return l.get(0); // l != null ? l.get(0) : new Recording();
	}

	public LoadThread(Cluster cluster, String bucketName, Collection collection,
			String[] keys, long runSeconds, int nRequestsPerSecond, long timeoutUs, long thresholdUs,
			CountDownLatch latch, Semaphore rateSemaphore, long[] baseTime, boolean logTimeout, boolean logMax,
			boolean logThreshold, boolean asObject, boolean kvGet, boolean kvInsert, int messageSize,
			Execution execution, int batchSize, boolean countMaxInParallel, boolean sameId) {
		this.keys = keys;
		this.runSeconds = runSeconds;
		this.nRequestsPerSecond = nRequestsPerSecond;
		this.timeoutUs = timeoutUs;
		this.thresholdUs = thresholdUs;
		this.latch = latch;
		this.rateSemaphore = rateSemaphore;
		this.baseTime = baseTime; // driver sets baseTime when it starts
		this.logTimeout = logTimeout;
		this.logMax = logMax;
		this.logThreshold = logThreshold;
		this.asObject = asObject;
		this.kvGet = kvGet;
		this.kvInsert = kvInsert;
		this.messageSize = messageSize;
		this.execution = execution;
		this.batchSize = batchSize;
		this.countMaxInParallel = countMaxInParallel;
		this.cluster = cluster;
		this.bucketName = bucketName;
		this.collection = collection;
		this.sameId = sameId;

		JsonObject messageJson = JsonObject.jo();
		for (int i = 0; messageJson.toBytes().length < messageSize - 10; i++) {
			String name = String.format("%1$" + 4 + "d", i).replace(" ", "0");
			String value = String.format("%1$1000s", "x");
			messageJson.put(name, value);
		}
		message = messageJson.toBytes();

	}

	static final AtomicLong requestsInParallel = new AtomicLong();
	public static final AtomicLong maxRequestsInParallel = new AtomicLong();

	public String getThreadName() {
		return Thread.currentThread().getName();
	}

	public void run() {

		//cluster.bucket(bucketName).waitUntilReady(Duration.ofSeconds(10));
		Scheduler pScheduler = null;
		long timeOffset = 0;
		try {

			CommonOptions options = kvGet ? GetOptions.getOptions().timeout(Duration.ofNanos(timeoutUs * 1000))
					: InsertOptions.insertOptions().timeout(Duration.ofNanos(timeoutUs * 1000));

			maxRecording = new Recording();
			recordings.put("timeouts", new LinkedList<>()); // linked list is cheaper to extend than ArrayList
			recordings.put("thresholds", new LinkedList<>()); // linked list is cheaper to extend than
			// ArrayList
			final String uuidStr = UUID.randomUUID().toString();
			final String uuid = uuidStr.substring(uuidStr.lastIndexOf("-") + 1);
			count = 0;
			sum = 0;

			if (execution.isReactive()) {
				// pScheduler = Schedulers.newParallel(Thread.currentThread().getName(), batchSize, false);
			}

			AtomicInteger reactiveCount = new AtomicInteger();
			endTime = System.currentTimeMillis() + runSeconds * 1000;
			while (System.currentTimeMillis() < endTime) {

				if (rateSemaphore != null) {
					try {
						rateSemaphore.acquire();
					} catch (InterruptedException e) {
						break;
					}
				}
				long t0 = System.nanoTime();
				timeOffset = System.currentTimeMillis() - baseTime[0];
				boolean timeoutOccurred = false;
				try {

					if (kvGet) {
						if (execution.isReactive()) {

							List<Object> mrList = Flux.range(1, batchSize).flatMap(i -> {
								if (countMaxInParallel
										&& requestsInParallel.incrementAndGet() > maxRequestsInParallel.get()) {
									maxRequestsInParallel.set(requestsInParallel.get());
								}
								count++;
								Mono<GetResult> mrMono = collection.reactive().get(keys[sameId ? 0 : count % keys.length],
										(GetOptions) options);
								return mrMono;
							}).map(result -> {
								if (countMaxInParallel)
									requestsInParallel.decrementAndGet();
								Object obj = decode(result);
								return obj;
							}).collectList().block();

						} else if (execution.isAsync()) {

							List<CompletableFuture<Object>> futures = new LinkedList<>();
							for (int i = 0; i < batchSize; i++) {
								if (countMaxInParallel
										&& requestsInParallel.incrementAndGet() > maxRequestsInParallel.get()) {
									maxRequestsInParallel.set(requestsInParallel.get());
								}
								count++;
								CompletableFuture<Object> f =
									collection.async().get(keys[sameId ? 0 : count % keys.length],
										(GetOptions) options).thenApply( result -> {
									Object obj = decode(result);
									if (countMaxInParallel)
										requestsInParallel.decrementAndGet();
									return obj;
								});
								futures.add(f);
							}
							CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

						} else {
							if (countMaxInParallel
									&& requestsInParallel.incrementAndGet() > maxRequestsInParallel.get()) {
								maxRequestsInParallel.set(requestsInParallel.get());
							}
							count++;
							GetResult r = collection.get(keys[count % keys.length], (GetOptions) options);
							if (countMaxInParallel)
								requestsInParallel.decrementAndGet();
							if (asObject)
								r.contentAsObject();
						}
					} else if (kvInsert) {
						if (execution.isReactive()) {
							reactiveCount.set(count);
							List<Optional<MutationToken>> mrList = Flux.range(1, batchSize).flatMap(i -> {
								if (countMaxInParallel
										&& requestsInParallel.incrementAndGet() > maxRequestsInParallel.get()) {
									maxRequestsInParallel.set(requestsInParallel.get());
								}
								Mono<MutationResult> mrMono = collection.reactive().insert(
										key(uuid, reactiveCount.getAndIncrement()), message, (InsertOptions) options);
								return mrMono;
							}).map(mr -> {
								if (countMaxInParallel)
									requestsInParallel.decrementAndGet();
								return mr.mutationToken();
							}).collectList().block();
							count = count + batchSize;
						} else {
							if (countMaxInParallel
									&& requestsInParallel.incrementAndGet() > maxRequestsInParallel.get()) {
								maxRequestsInParallel.set(requestsInParallel.get());
							}
							MutationResult mr = collection.insert(key(uuid, count++), message, (InsertOptions) options);
							if (countMaxInParallel)
								requestsInParallel.decrementAndGet();
							mr.mutationToken();
						}
					} else {
						count++;
						QueryResult qr = cluster.query("SELECT * from `travel-sample` where id = ?",
								QueryOptions.queryOptions()
										.parameters(JsonArray.create().add(keys[sameId ? 0 : count % keys.length].split("_")[1])));
						qr.rowsAsObject();
					}
				} catch (UnambiguousTimeoutException e) {
					timeoutOccurred = true;
				}
				long rTime = System.nanoTime() - t0;
				sum += (rTime * batchSize);
				if (rTime > maxRecording.value) {
					maxRecording = new Recording(getThreadName(), "mx", count, rTime, timeOffset);
					List<Recording> l = new LinkedList();
					l.add(maxRecording);
					recordings.put("max", l);
					if (logMax)
						System.out.println(maxRecording);
				}
				if (timeoutOccurred) {
					Recording timeout = new Recording(getThreadName(), "TO", count, rTime, timeOffset);
					recordings.get("timeouts").add(timeout);
					if (logTimeout)
						System.out.println(timeout);
				} else if (rTime > thresholdUs * 1000) { // if already recorded timeout, don't also record threshold
					Recording threshold = new Recording(getThreadName(), rTime > timeoutUs * 1000 ? "TH" : "th", count,
							rTime, timeOffset);
					recordings.get("thresholds").add(threshold);
					if (logThreshold)
						System.out.println(threshold);
				}
			}
		} catch (RuntimeException t) {
			if (t.getCause() != null && !(t.getCause() instanceof InterruptedException) || rateSemaphore == null)
				throw t;
		} finally {
			if (pScheduler != null) {
				pScheduler.disposeGracefully();
			}
			if (count > 0) {
				recordings.put("average", new LinkedList<Recording>());
				recordings.get("average").add(new Recording(getThreadName(), "avg", count, sum / count, 999999999));
			}
			latch.countDown();
			if (collection == null) {
				cluster.close();
			}
		}
	}

	String key(String uuid, long count) {
		return uuid + String.format("%1$8d", count).replace(" ", "0");
	}

	Object decode(GetResult result){
		if(!asObject){
			return EMPTY_JSON_OBJECT;
		}
		Transcoder tc = cluster.environment().transcoder();
		if(tc == RawStringTranscoder.INSTANCE )
			return result.contentAs(String.class);

		if(tc == RawJsonTranscoder.INSTANCE  || tc == RawBinaryTranscoder.INSTANCE)
			return result.contentAs(byte[].class);

		if(cluster.environment().transcoder() == SerializableTranscoder.INSTANCE )
			return result.contentAsObject(); // it was saved as a String

		return result.contentAsObject();
	}

	Cluster cluster(){
		return cluster;
	}
}
