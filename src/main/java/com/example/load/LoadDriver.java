package com.example.load;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import ch.qos.logback.classic.Level;
import java.util.logging.Logger;


import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class LoadDriver {
	static Logger logger = Logger.getLogger("com.couchbase");
	public static void main(String[] args) {
		//ch.qos.logback.classic.Logger l=(ch.qos.logback.classic.Logger)logger;
		//l.setLevel(Level.INFO);

		int argc = 0;
		int nThreads = 4;
		int nRequestsPerSecond = 0;
		int runSeconds = 10;
		int timeoutUs = 40000;
		long thresholdUs = -1;
		int nKvConnections = 1;
		boolean logTimeout=true;
		boolean logMax=false;
		boolean logThreshold=true;
		boolean asContent=false;
		long gcIntervalMs=0;
		String username = "Administrator";
		String password = "password";
		String cbUrl = "localhost";
		String bucketname = "travel-sample";
		String keys[] = { "airport_1254" };
		String fetchType = "kv";
		List<String> keysList=new ArrayList<>();

		while (argc < args.length) {
			if ("--threads".equals(args[argc]))
				nThreads = Integer.parseInt(args[++argc]);
			else if ("--requestspersecond".equals(args[argc]))
				nRequestsPerSecond = Integer.parseInt(args[++argc]);
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
			else if ("--logthreshold".equals(args[argc]))
				logThreshold = Boolean.valueOf(args[++argc]);
			else if ("--ascontent".equals(args[argc]))
				asContent = Boolean.valueOf(args[++argc]);
			else if ("--fetchtype".equals(args[argc]))
				fetchType = args[++argc];
			else {
				usage();
				System.err.println(" unsupported option: "+args[argc]);
				System.exit(1);
			}
			argc++;
		}

		if(thresholdUs <= 0)
			thresholdUs = timeoutUs / 5;

		if(!keysList.isEmpty())
			keys = (String[])keysList.toArray(new String[]{});

		ClusterEnvironment env = ClusterEnvironment.builder()
				.ioConfig(IoConfig.numKvConnections(nKvConnections))
				.build();
		ClusterOptions options = ClusterOptions.clusterOptions(username, password);

		Cluster cluster = Cluster.connect(cbUrl, options.environment(env));
		Bucket bucket = cluster.bucket(bucketname);
		Collection collection = bucket.defaultCollection();
		bucket.waitUntilReady(Duration.ofSeconds(10));
		//printClusterEndpoints(cluster);

		logger.info("Connected");

		// warm things up by running full-bore (no rateSemaphore) for two seconds. No logging.
		// execute the run() methods in ThreadWrapper() threads
		// so that they can be executed concurrently with ThreadWrapper.start()
		// instead of thread[i].start() [ a thread can only have start() called once ]

		LoadThread[] threads = new LoadThread[nThreads];
		CountDownLatch latch = new CountDownLatch(nThreads);

		long[] baseTime = new long[1];
		for (int i = 0; i < nThreads; i++) {
			// we'll run for 2 seconds before making our measurement, not using rateSemaphore
			threads[i] = new LoadThread(collection, cbUrl, username, password, bucketname, keys, 2, nRequestsPerSecond, timeoutUs,
					thresholdUs, latch, null, baseTime, false, false, false, asContent, !fetchType.equals("query"), cluster);
			(new ThreadWrapper(threads[i])).start();
		}

		try {
			System.out.println("waiting for warm-up");
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("wait for idle Endpoints to timeout");
		sleep(4000); // wait for idle Endpoints to timeout
		System.out.println("========================================================");
		latch = new CountDownLatch(nThreads);
		Semaphore rateSemaphore = nRequestsPerSecond == 0 ? null :  new Semaphore(0);
		baseTime[0]=System.currentTimeMillis();

		if(gcIntervalMs != 0)
			new GCThread( gcIntervalMs, runSeconds, baseTime[0] ).start();

		for (int i = 0; i < nThreads; i++) {
			threads[i].setRunSeconds(runSeconds);
			threads[i].setLatch(latch);
			threads[i].setRateSemaphore(rateSemaphore);
			threads[i].setLogMax(logMax);
			threads[i].setLogThreshold(logThreshold);
			threads[i].setLogTimeout(logTimeout);
			sleep(1);  //stagger the start
			threads[i].start();
		}
		System.out.println(cluster.environment());
		System.out.println("              ...running...");
		if(rateSemaphore!=null) { // if rate-limited produce on counting semaphore at requested rate
			long endTime = baseTime[0] + runSeconds * 1000;
			long nRequests = 0;
			while (rateSemaphore != null && System.currentTimeMillis() < endTime) {
				while (nRequests < (System.currentTimeMillis() - baseTime[0]) * nRequestsPerSecond / 1000) {
					rateSemaphore.release();
					nRequests++;
				}
				sleep(2);
			}
			for (int i = 0; i < threads.length; i++) // kill threads waiting for rateSemaphore
				threads[i].interrupt();
		}

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("===================  RESULTS  ========================");

		long count = 0;
		Recording max = new Recording() ;
		List<Recording> allRecordings=new ArrayList<>();
		long sum=0;
		for (int i = 0; i < nThreads; i++) {
			allRecordings.addAll(threads[i].getRecordings("timeouts"));
			allRecordings.addAll(threads[i].getRecordings("thresholds"));
			Recording avg=threads[i].getRecording("average");
			// aggregate the counts
			count += threads[i].getCount();
			// reconstitute the total execution time and aggregate
			sum = sum +avg.count * avg.value;
			// save the max of all threads
			max = threads[i].getRecording("max").getValue() > max.getValue() ? threads[i].getRecording("max") : max;
		}

		// sort recordings chronologically
		allRecordings.sort(new Comparator<Recording>()
		{ public int compare(Recording a, Recording b){
			if(a.timeOffset == b.timeOffset)
				return a.name.compareTo(b.name);
			return (int)(a.timeOffset - b.timeOffset); }});

		for(Recording recording:allRecordings){
			System.out.println(recording);
		}
		/*
		Recording rr=allRecordings.stream().filter(r -> r.name.equals("avg")).reduce(new Recording(), (a,b) ->
				new Recording("","sum",a.count+b.count,a.value+b.value*b.count,0) );
		 */
		System.out.println("MAX: "+max);
		System.out.println("========================================================");
		//printClusterEndpoints(cluster);
		System.out.printf("Run: seconds: %d, threads: %d, timeout: %dus, threshold: %dus requests/second: %d %s, forced GC interval: %dms\n",
				runSeconds, threads.length, timeoutUs, thresholdUs , nRequestsPerSecond, nRequestsPerSecond == 0 ? "(max)":"", gcIntervalMs);
		System.out.printf("count: %d, requests/second: %d, max: %.0fus avg: %dus, aggregate rq/s: %d\n",
				count , count / runSeconds , max.getValue()/1000.0, sum/1000/count, 1000000000/(sum/count/threads.length));

	}

	private static void printClusterEndpoints(Cluster cluster) {
		for(Map.Entry<ServiceType,List<EndpointDiagnostics>> entry: cluster.diagnostics().endpoints().entrySet()){
			for(EndpointDiagnostics ed: entry.getValue()){
				if(ed.type().toString().equals("KV"))
					System.out.println(ed);
			}
		}
	}

	private static void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}

	public static void usage(){
		System.err.println("LoadDriver ");
		System.err.println("	--threads <n>");
		System.err.println("	--runseconds <n>");
		System.err.println("	--requestspersecond <n>");
		System.err.println("	--timeoutmicroseconds <n>");
		System.err.println("	--thresholdmicroseconds <n>");
		System.err.println("	--gcintervalmilliseconds <n>");
		System.err.println("	--url <url>");
		System.err.println("	--username <username>");
		System.err.println("	--password <password>");
		System.err.println("	--bucket <bucket>");
		System.err.println("	--logtimeout <true|false>");
		System.err.println("	--logmax <true|false>");
		System.err.println("	--logthreshold <true|false>");
		System.err.println("	--key <key> [ --key <key> ...]");
		System.err.println("  --fetchtype [ kv | query ] # CREATE INDEX `def_id` ON `travel-sample`(`id`) ");
	}

	// Thread that runs the run() of another Thread
	public static class ThreadWrapper extends Thread {
		Thread thread;
		public ThreadWrapper(Thread thread){
			this.thread = thread;
		}
		public void run() {thread.run();}
	}
}
