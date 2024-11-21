package com.example.load;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.codec.SerializableTranscoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class LoadDriver {
	static Logger logger = Logger.getLogger("com.couchbase.core");

	public static void main(String[] args) {
		int argc = 0;
		int nThreads = 4;
		int nRequestsPerSecond = 0;
		int runSeconds = 10;
		int timeoutUs = 2500000;
		int messageSize = 2;
		int schedulerThreadCount = 0;
		Execution execution = Execution.reactive;
		Transcoder transcoder = Transcoder.json;
		int batchSize = 128;
		long thresholdUs = -1;
		int nKvConnections = 2;
		boolean logTimeout = true;
		boolean logMax = false;
		boolean logThreshold = true;
		boolean asObject = true;
		boolean countMaxInParallel = false;
		boolean virtualThreads = false;
		boolean shareCluster = true;
		long gcIntervalMs = 0;
		int kvEventLoopThreadCount = 0;
		String username = "Administrator";
		String password = "password";
		String cbUrl = "localhost";
		String bucketname = "travel-sample";
		String keys[] = null;
		String operationType = "get";
		List<String> operationTypes = List.of("get", "insert", "query");
		List<String> keysList = new ArrayList<>();

		while (argc < args.length) {
			if ("--threads".equals(args[argc]))
				nThreads = Integer.parseInt(args[++argc]);
			else if ("--requestspersecond".equals(args[argc]))
				nRequestsPerSecond = Integer.parseInt(args[++argc]);
			else if ("--kveventloopthreadcount".equals(args[argc]))
				kvEventLoopThreadCount = Integer.parseInt(args[++argc]);
			else if ("--runseconds".equals(args[argc]))
				runSeconds = Integer.parseInt(args[++argc]);
			else if ("--timeoutmicroseconds".equals(args[argc]))
				timeoutUs = Integer.parseInt(args[++argc]);
			else if ("--thresholdmicroseconds".equals(args[argc]))
				thresholdUs = Integer.parseInt(args[++argc]);
			else if ("--gcintervalmilliseconds".equals(args[argc]))
				gcIntervalMs = Integer.parseInt(args[++argc]);
			else if ("--kvconnections".equals(args[argc]))
				nKvConnections = Integer.parseInt(args[++argc]);
			else if ("--messagesize".equals(args[argc]))
				messageSize = Integer.parseInt(args[++argc]);
			else if ("--schedulerthreadcount".equals(args[argc]))
				schedulerThreadCount = Integer.parseInt(args[++argc]);
			else if ("--batchsize".equals(args[argc]))
				batchSize = Integer.parseInt(args[++argc]);
			else if ("--execution".equals(args[argc]))
				execution = Execution.valueOf(args[++argc]);
			else if ("--transcoder".equals(args[argc]))
				transcoder = Transcoder.valueOf(args[++argc]);
			else if ("--virtualthreads".equals(args[argc]))
				virtualThreads = Boolean.valueOf(args[++argc]);
			else if ("--url".equals(args[argc]))
				cbUrl = args[++argc];
			else if ("--username".equals(args[argc]))
				username = args[++argc];
			else if ("--password".equals(args[argc]))
				password = args[++argc];
			else if ("--bucket".equals(args[argc]))
				bucketname = args[++argc];
			else if ("--key".equals(args[argc]))
				keysList.add(args[++argc]);
			else if ("--logtimeout".equals(args[argc]))
				logTimeout = Boolean.valueOf(args[++argc]);
			else if ("--logmax".equals(args[argc]))
				logMax = Boolean.valueOf(args[++argc]);
			else if ("--countmaxinparallel".equals(args[argc]))
				countMaxInParallel = Boolean.valueOf(args[++argc]);
			else if ("--logthreshold".equals(args[argc]))
				logThreshold = Boolean.valueOf(args[++argc]);
			else if ("--asobject".equals(args[argc]))
				asObject = Boolean.valueOf(args[++argc]);
			else if ("--sharecluster".equals(args[argc]))
				shareCluster = Boolean.valueOf(args[++argc]);
			else if ("--operationtype".equals(args[argc])) {
				operationType = args[++argc];
				if (!operationTypes.contains(operationType)) {
					throw new RuntimeException("operation type must be one of " + operationTypes);
				}
			} else {
				usage();
				System.err.println(" unsupported option: " + args[argc]);
				System.exit(1);
			}
			argc++;
		}

		if (thresholdUs <= 0)
			thresholdUs = timeoutUs / 5;

		if (!keysList.isEmpty())
			keys = keysList.toArray(new String[] {});

		Cluster cluster = getCluster(cbUrl, username, password, bucketname, nKvConnections, kvEventLoopThreadCount,
				schedulerThreadCount, thresholdUs, transcoder);
		Collection collection = cluster.bucket(bucketname).defaultCollection();

		logger.info("Connected");
		JsonObject messageJson = JsonObject.jo();
		byte[] message;
		for (int i = 0; messageJson.toBytes().length < messageSize - 10; i++) {
			String name = String.format("%1$" + 4 + "d", i).replace(" ", "0");
			String value = String.format("%1$1000s", "x");
			messageJson.put(name, value);
		}
		message = messageJson.toBytes();
		System.err.println("Inserted: message length is: " + message.length);
		if (keys == null) {
			keys = new String[batchSize];
			for (int i = 0; i < batchSize; i++) {
				keys[i] = "000" + i;
			}
		}
		if (operationType.equals("get")) {
			Cluster cl = cluster;
			Duration to = Duration.ofMillis(timeoutUs / 1000L);
			AtomicInteger inflight = new AtomicInteger();
			Flux.fromIterable(List.of(keys)).parallel().runOn(Schedulers.boundedElastic()).flatMap(k -> {
				ExistsResult exr = collection.exists(k, ExistsOptions.existsOptions().timeout(to));
				if (exr.exists()) {
					collection.remove(k, RemoveOptions.removeOptions().timeout(to));
				}
				com.couchbase.client.java.codec.Transcoder tc = cl.environment().transcoder();
				collection.insert(k,
						tc == RawJsonTranscoder.INSTANCE || tc == RawStringTranscoder.INSTANCE ? message : messageJson,
						InsertOptions.insertOptions().timeout(to));
				inflight.decrementAndGet();
				return Flux.empty();
			}).sequential().blockLast();
		}

		if (!shareCluster) {
			cluster.close();
			cluster = null;
		}

		// printClusterEndpoints(cluster);

		// warm things up by running full-bore (no rateSemaphore) for two seconds. No logging.
		// execute the run() methods in ThreadWrapper() threads
		// so that they can be executed concurrently with ThreadWrapper.start()
		// instead of thread[i].start() [ a thread can only have start() called once ]

		LoadThread[] loads = new LoadThread[nThreads];
		Thread[] threads = new Thread[nThreads];
		CountDownLatch latch = new CountDownLatch(nThreads);

		long[] baseTime = new long[1];
		for (int i = 0; i < nThreads; i++) {
			// we'll run for 2 seconds before making our measurement, not using rateSemaphore
			Cluster theCluster = shareCluster ? cluster
					: getCluster(cbUrl, username, password, bucketname, nKvConnections, kvEventLoopThreadCount,
							schedulerThreadCount, thresholdUs, transcoder);
			Collection theCollection = theCluster.bucket(bucketname).defaultCollection();
			loads[i] = new LoadThread(theCluster, bucketname, theCollection, keys, 2, nRequestsPerSecond, timeoutUs, thresholdUs,
					latch, null, baseTime, false, false, false, asObject, operationType.equals("get"),
					operationType.equals("insert"), messageSize, execution, batchSize, countMaxInParallel);
			(new ThreadWrapper(loads[i], virtualThreads)).start();
		}

		try {
			System.out.println("waiting for warm-up");
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("wait for idle Endpoints to timeout");
		sleep(4000); // wait for idle Endpoints to timeout
		latch = new CountDownLatch(nThreads);
		Semaphore rateSemaphore = nRequestsPerSecond == 0 ? null : new Semaphore(0);
		baseTime[0] = System.currentTimeMillis();

		if (gcIntervalMs != 0)
			new GCThread(gcIntervalMs, runSeconds, baseTime[0]).start();

		for (int i = 0; i < nThreads; i++) {
			loads[i].setRunSeconds(runSeconds);
			loads[i].setLatch(latch);
			loads[i].setRateSemaphore(rateSemaphore);
			loads[i].setLogMax(logMax);
			loads[i].setLogThreshold(logThreshold);
			loads[i].setLogTimeout(logTimeout);
			threads[i] = new ThreadWrapper(loads[i], virtualThreads);
			sleep(1); // stagger the start
			threads[i].start();
		}
		if (cluster != null) {
			System.out.println(cluster.environment());
		}
		System.out.println("              ...running...");
		if (rateSemaphore != null) { // if rate-limited produce on counting semaphore at requested rate
			long endTime = baseTime[0] + runSeconds * 1000;
			long nRequests = 0;
			while (rateSemaphore != null && System.currentTimeMillis() < endTime) {
				while (nRequests < (System.currentTimeMillis() - baseTime[0]) * nRequestsPerSecond / 1000) {
					rateSemaphore.release();
					nRequests++;
				}
				sleep(2);
			}
			for (int i = 0; i < loads.length; i++) { // kill threads waiting for rateSemaphore
				rateSemaphore.drainPermits();
				threads[i].interrupt();
			}
		}

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("===================  RESULTS  ========================");
		if (countMaxInParallel)
			System.out.println("maxInRequestsInParallel: " + LoadThread.maxRequestsInParallel);

		long count = 0;
		long threadCount = 0;
		Recording max = new Recording();
		List<Recording> allRecordings = new ArrayList<>();
		long sum = 0;
		for (int i = 0; i < nThreads; i++) {
			if (loads[i].getCount() == 0)
				continue;
			threadCount++;
			allRecordings.addAll(loads[i].getRecordings("timeouts"));
			allRecordings.addAll(loads[i].getRecordings("thresholds"));
			Recording avg = loads[i].getRecording("average");
			// aggregate the counts
			count += loads[i].getCount();
			// reconstitute the total execution time and aggregate
			sum = sum + avg.count * avg.value;
			// save the max of all threads
			max = loads[i].getRecording("max").getValue() > max.getValue() ? loads[i].getRecording("max") : max;
		}

		// sort recordings chronologically
		allRecordings.sort(new Comparator<Recording>() {
			public int compare(Recording a, Recording b) {
				if (a.timeOffset == b.timeOffset)
					return a.name.compareTo(b.name);
				return (int) (a.timeOffset - b.timeOffset);
			}
		});

		for (Recording recording : allRecordings) {
			System.out.println(recording);
		}
		/*
		Recording rr=allRecordings.stream().filter(r -> r.name.equals("avg")).reduce(new Recording(), (a,b) ->
				new Recording("","sum",a.count+b.count,a.value+b.value*b.count,0) );
		 */
		System.out.println("MAX: " + max);
		System.out.println("========================================================");
		// printClusterEndpoints(cluster);
		System.out.printf(
				"Run: seconds: %d, threads: %d, timeout: %dus, threshold: %dus requests/second: %d %s, forced GC interval: %dms, execution: %s, batchSize: %d\n",
				runSeconds, loads.length, timeoutUs, thresholdUs, nRequestsPerSecond,
				nRequestsPerSecond == 0 ? "(max)" : "", gcIntervalMs, execution, !execution.isSync() ? batchSize : 1);
		System.out.printf("count: %d, requests/second: %d, max: %.0fus avg: %dus, rq/s per-thread: %d threads: %d\n",
				count, count / runSeconds, max.getValue() / 1000.0, sum / 1000 / count,
				count / runSeconds / threadCount, nThreads);

		System.err.println("sum: " + sum);
		System.err.println("count: " + count);
		System.err.println("threads: " + threadCount);
		if (cluster != null) {
			cluster.close();
		}
		System.exit(0); // had to add this after adding pScheduler in LoaqThread
	}

	static Cluster getCluster(String cbUrl, String username, String password, String bucketname, int nKvConnections,
			int kvEventLoopThreadCount, int schedulerThreadCount, long thresholdUs, Transcoder transcoder) {

		ThresholdLoggingTracerConfig.Builder config = ThresholdLoggingTracerConfig.builder()
				.kvThreshold(Duration.ofMillis(thresholdUs / 1000));

		ClusterEnvironment.Builder builder = ClusterEnvironment.builder().ioEnvironment(ioe -> {
			if (kvEventLoopThreadCount > 0)
				ioe.eventLoopThreadCount(kvEventLoopThreadCount);
		}).ioConfig(io -> io.numKvConnections(nKvConnections));
		if (schedulerThreadCount != 0) {
			builder.schedulerThreadCount(schedulerThreadCount);
		}
		if (cbUrl.startsWith("couchbases")) {
			builder.securityConfig(sc -> sc.enableTls(true));
		}
		if (!transcoder.isJsonTranscoder()) {
			builder.transcoder(transcoder.getInstance());
		}
		builder.thresholdLoggingTracerConfig(config).build();
		ClusterEnvironment env = builder.build();
		ClusterOptions options = ClusterOptions.clusterOptions(username, password).environment(env);
		Cluster cluster = Cluster.connect(cbUrl, options.environment(env));
		Bucket bucket = cluster.bucket(bucketname);
		return cluster;
	}

	private static void printClusterEndpoints(Cluster cluster) {
		for (Map.Entry<ServiceType, List<EndpointDiagnostics>> entry : cluster.diagnostics().endpoints().entrySet()) {
			for (EndpointDiagnostics ed : entry.getValue()) {
				if (ed.type().toString().equals("KV"))
					System.out.println(ed);
			}
		}
	}

	private static void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {}
	}

	public static void usage() {
		System.err.println("LoadDriver ");
		System.err.println("	--threads <n>");
		System.err.println("	--runseconds <n>");
		System.err.println("	--requestspersecond <n>");
		System.err.println("	--timeoutmicroseconds <n>");
		System.err.println("	--thresholdmicroseconds <n>");
		System.err.println("	--gcintervalmilliseconds <n>");
		System.err.println("	--kvconnections <n>");
		System.err.println("	--messagesize <n>");
		System.err.println("	--schedulerthreadcount <n>");
		System.err.println("	--kveventloopthreadcount <n>");
		System.err.println("	--batchSize <n>");
		System.err.println("	--execution <reactive|async|sync>");
		System.err.println("	--virtualthreads <true|false>");
		System.err.println("	--url <url>");
		System.err.println("	--username <username>");
		System.err.println("	--password <password>");
		System.err.println("	--bucket <bucket>");
		System.err.println("	--logtimeout <true|false>");
		System.err.println("	--sharecluster <true|false>");
		System.err.println("	--logmax <true|false>");
		System.err.println("	--countmaxinparallel <true|false>");
		System.err.println("	--logthreshold <true|false>");
		System.err.println("	--asobject <true|false>");
		System.err.println("	--transcoder rawjson|rawbinary|rawstring|serializable");
		System.err.println("	--key <key> [ --key <key> ...]");
		System.err.println(
				"	--operationtype [ get | insert | query ] # CREATE INDEX `def_id` ON `travel-sample`(`id`) ");
	}

	// Thread that runs the run() of another Thread
	public static class ThreadWrapper extends Thread {
		Thread thread;

		public ThreadWrapper(LoadThread runnable, boolean virtualThreads) {
			Object platformOrVirtual = callMethod(Thread.class, virtualThreads ? "ofVirtual" : "ofPlatform");
			if (platformOrVirtual != null) {
				Object factory = null;
				try {
					factory = callMethod(Class.forName("java.lang.Thread$Builder"), platformOrVirtual, "factory");
					this.thread = (Thread) callMethod(Class.forName("java.util.concurrent.ThreadFactory"), factory,
							"newThread", new Class<?>[] { Runnable.class }, runnable);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			} else if (!virtualThreads) {
				this.thread = new Thread(runnable);
			} else {
				throw new RuntimeException("virtualThreads not implemented");
			}
		}

		public void start() {
			thread.start();
		}

		private static Object callMethod(Class<?> clazz, String methodName) {
			return callMethod(clazz, null, methodName, null, null);
		}

		private static Object callMethod(Class<?> clazz, Object o, String methodName) {
			return callMethod(clazz, o, methodName, null, null);
		}

		private static Object callMethod(Class<?> clazz, Object o, String methodName, Class<?>[] argTypes,
				Object... args) {
			Method m = null;
			try {
				m = (clazz != null ? clazz : o.getClass()).getMethod(methodName, argTypes);
			} catch (NoSuchMethodException nsme) {
				throw new RuntimeException(nsme);
			}
			try {
				// System.err.println("calling "+methodName);
				return m.invoke(o, args);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public enum Transcoder {
		json(null), rawjson(RawJsonTranscoder.INSTANCE), rawbinary(RawBinaryTranscoder.INSTANCE), rawstring(
				RawStringTranscoder.INSTANCE), serializable(SerializableTranscoder.INSTANCE);

		com.couchbase.client.java.codec.Transcoder instance;

		boolean isJsonTranscoder() {
			return instance == null;
		}

		com.couchbase.client.java.codec.Transcoder getInstance() {
			return instance;
		}

		Transcoder(com.couchbase.client.java.codec.Transcoder tc) {
			instance = tc;
		}
	};
}
