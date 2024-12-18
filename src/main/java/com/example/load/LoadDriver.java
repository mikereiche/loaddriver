package com.example.load;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig;
import com.couchbase.client.core.service.ServiceType;
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
		boolean sameId = false;
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
			else if ("--sameid".equals(args[argc]))
				sameId = Boolean.valueOf(args[++argc]);
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
				schedulerThreadCount, thresholdUs, transcoder, shareCluster ? nThreads : 0);
		System.err.println(cluster.environment());
		Collection collection = cluster.bucket(bucketname).defaultCollection();

		Object document;
		if (transcoder.isJsonTranscoder() || transcoder.isSerializableTranscoder()
				|| transcoder.isRawJsonTranscoder()) {
			JsonObject messageJson = JsonObject.jo();
			byte[] someBytes = new byte[1000];
			Random random = new Random();
			for (int i = 0; messageJson.toBytes().length < messageSize - 10; i++) {
				String name = String.format("%1$4d", i).replace(" ", "0");
				random.nextBytes(someBytes);
				String value = new String(asciify(someBytes));
				messageJson.put(name, value);
			}

			if (transcoder.isRawJsonTranscoder()) {
				document = messageJson.toBytes();
			} else {
				document = messageJson;
			}
		} else if (transcoder.isRawStringTranscoder() || transcoder.isRawBinaryTranscoder()) {
			byte[] someBytes = new byte[messageSize];
			new Random().nextBytes(someBytes);
			if (transcoder.isRawStringTranscoder()) {
				document = new String(asciify(someBytes));
			} else {
				document = someBytes;
			}
		} else {
			throw new RuntimeException("unknown transcoder " + transcoder);
		}

		System.err.println(
				"encoded message length is: " + transcoder.instance != null ? transcoder.instance.encode(document).encoded().length
						: document.toString().length());

		if (keys == null) {
			keys = new String[batchSize];
			for (int i = 0; i < batchSize; i++) {
				keys[i] = String.format("%05d", i);
			}
		}
		if (operationType.equals("get")) {
			Duration to = Duration.ofMillis(timeoutUs / 1000L);
			Flux.fromIterable(List.of(keys)).parallel().runOn(Schedulers.boundedElastic()).flatMap(k -> {
				ExistsResult exr = collection.exists(k, ExistsOptions.existsOptions().timeout(to));
				if (exr.exists()) {
					collection.remove(k, RemoveOptions.removeOptions().timeout(to));
				}
				collection.insert(k, document, InsertOptions.insertOptions().timeout(to));
				return Flux.empty();
			}).sequential().blockLast();
		}

		if (!shareCluster) {
			cluster.close();
			cluster = null;
		}

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
							schedulerThreadCount, thresholdUs, transcoder, 0);
			Collection theCollection = theCluster.bucket(bucketname).defaultCollection();
			loads[i] = new LoadThread(theCluster, bucketname, theCollection, keys, 2, nRequestsPerSecond, timeoutUs,
					thresholdUs, latch, null, baseTime, false, false, false, asObject, operationType.equals("get"),
					operationType.equals("insert"), document, execution, batchSize, countMaxInParallel, sameId);
			(new ThreadWrapper(loads[i], virtualThreads)).start();
		}

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		sleep(4000); // wait for idle Endpoints to timeout
		latch = new CountDownLatch(nThreads);
		Semaphore rateSemaphore = nRequestsPerSecond == 0 ? null : new Semaphore(0);
		baseTime[0] = System.currentTimeMillis();

		if (gcIntervalMs != 0)
			new GCThread(gcIntervalMs, runSeconds, baseTime[0]).start();

		System.out.println("Running: ");
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

		long count = 0;

		Recording max = new Recording();
		List<Recording> allRecordings = new ArrayList<>();
		long sum = 0;

		if (operationType.equals("get")) {
			Duration to = Duration.ofMillis(timeoutUs / 1000L);
			Flux.fromIterable(List.of(keys)).parallel().runOn(Schedulers.boundedElastic()).flatMap(k -> {
				collection.remove(k, RemoveOptions.removeOptions().timeout(to));
				return Flux.empty();
			}).sequential().blockLast();
		}

		if (cluster != null) {
			cluster.close();
			cluster = null;
		}
		for (int i = 0; i < nThreads; i++) {
			if (!shareCluster) {
				loads[i].cluster().close();
			}
			if (loads[i].getCount() == 0)
				continue;

			allRecordings.addAll(loads[i].getRecordings("timeouts"));
			allRecordings.addAll(loads[i].getRecordings("thresholds"));
			Recording avg = loads[i].getRecording("average");
			// aggregate the counts
			count += loads[i].getCount();
			// reconstitute the total execution time and aggregate
			sum = sum + avg.count * avg.value;
			// save the max of all threads
			Recording loadMax = loads[i].getRecording("max");
			if (loadMax != null) {
				max = loadMax.getValue() > max.getValue() ? loadMax : max;
			}
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

		StringBuffer p = new StringBuffer();
		p.append(" nthreads: " + nThreads);
		p.append(", nRequestsPerSecond: " + nRequestsPerSecond);
		p.append(", kvEventLoopThreadCount: " + kvEventLoopThreadCount);
		p.append(", runSeconds: " + runSeconds);
		p.append(", timeoutUs: " + timeoutUs);
		p.append(", thresholdUs: " + thresholdUs);
		p.append(", gcIntervalMs: " + gcIntervalMs);
		p.append(", nKvConnections: " + nKvConnections);
		p.append(", messageSize: " + messageSize);
		p.append(", schedulerThreadCount: " + schedulerThreadCount);
		p.append(", batchSize: " + batchSize);
		p.append(", execution: " + execution);
		p.append(", transcoder: " + transcoder);
		p.append(", virtualThreads: " + virtualThreads);
		p.append(", cbUrl: " + cbUrl);
		p.append(", bucketname: " + bucketname);
		// p.append(", keysList: " + keysList);
		p.append(", asObject: " + asObject);
		p.append(", sameId: " + sameId);
		p.append(", shareCluster: " + shareCluster);
		p.append(", operationType: " + operationType);

		StringBuffer r = new StringBuffer();
		r.append("count: " + count);
		r.append(", requests/second: " + count / runSeconds);
		r.append(", max: " + max.getValue() / 1000);
		r.append(", avg: " + sum / 1000 / count);
		r.append(", rq/s/thread: " + count / runSeconds / nThreads);

		if (countMaxInParallel)
			r.append(", maxInRequestsInParallel: " + LoadThread.maxRequestsInParallel);

		System.out.println(r + ", " + p);

	}

	private static byte[] asciify(byte[] someBytes) {
		for (int i = 0; i < someBytes.length; i++) {
			someBytes[i] = (byte) (Math.abs(someBytes[i]) % ('z' - ' ') + ' ');
		}
		return someBytes;
	}

	static Cluster getCluster(String cbUrl, String username, String password, String bucketName, int nKvConnections,
			int kvEventLoopThreadCount, int schedulerThreadCount, long thresholdUs, Transcoder transcoder,
			int maxHttpConnections) {

		ClusterEnvironment.Builder builder = ClusterEnvironment.builder();

		// builder.transactionsConfig( tc -> tc.cleanupConfig( cc ->
		// cc.cleanupLostAttempts(false).cleanupClientAttempts(false) ));
		// builder.ioConfig( ioc -> ioc.captureTraffic(ServiceType.KV));

		builder.ioConfig(io -> io.numKvConnections(nKvConnections));

		if (kvEventLoopThreadCount > 0) {
			builder.ioEnvironment(ioe -> ioe.eventLoopThreadCount(kvEventLoopThreadCount));
		}

		if (schedulerThreadCount != 0) {
			builder.schedulerThreadCount(schedulerThreadCount);
		}
		if (cbUrl.startsWith("couchbases")) {
			builder.securityConfig(sc -> sc.enableTls(true));
		}
		if (!transcoder.isJsonTranscoder()) {
			builder.transcoder(transcoder.getInstance());
		}
		if (maxHttpConnections != 0) {
			builder.ioConfig(ioc -> ioc.maxHttpConnections(maxHttpConnections));
		}

		ThresholdLoggingTracerConfig.Builder config = ThresholdLoggingTracerConfig.builder()
				.kvThreshold(Duration.ofMillis(thresholdUs / 1000));
		builder.thresholdLoggingTracerConfig(config).build();

		ClusterEnvironment env = builder.build();
		ClusterOptions options = ClusterOptions.clusterOptions(username, password).environment(env);
		Cluster cluster = Cluster.connect(cbUrl, options.environment(env));
		cluster.bucket(bucketName).waitUntilReady(Duration.ofSeconds(10));
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
		System.err.println("	--sameid <true|false>");
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

		public boolean isRawStringTranscoder() {
			return this == rawstring;
		}

		public boolean isRawJsonTranscoder() {
			return this == rawjson;
		}

		public boolean isSerializableTranscoder() {
			return this == serializable;
		}

		public boolean isRawBinaryTranscoder() {
			return this == rawbinary;
		}
	};
}
