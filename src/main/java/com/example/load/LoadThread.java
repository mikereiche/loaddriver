package com.example.load;

import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class LoadThread extends Thread {
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
	boolean asContent;

	long count = 0;
	long sum = 0;
	Recording maxRecording;

	Cluster cluster;
	Bucket bucket;
	Collection collection;

	public HashMap<String, List<Recording>> recordings = new HashMap<>();

	public long getCount(){
		return count;
	}

	public void setRunSeconds(long runSeconds){
		this.runSeconds = runSeconds;
	}
	public void setRateSemaphore(Semaphore rateSemaphore){
		this.rateSemaphore = rateSemaphore;
	}
	public void setLatch(CountDownLatch latch){
		this.latch = latch;
	}
	public void setLogMax(boolean logMax){
		this.logMax = logMax;
	}
	public void setLogThreshold(boolean logThreshold){
		this.logThreshold = logThreshold;
	}
	public void setLogTimeout(boolean logTimeout){
		this.logTimeout = logTimeout;
	}

	public List<Recording> getRecordings(String key) {
		return recordings.get(key);
	}

	public Recording getRecording(String key) {
		return recordings.get(key).get(0);
	}

	public LoadThread(Collection collection, String cbUrl, String username, String password, String bucketname, String[] keys, long runSeconds,
			int nRequestsPerSecond, long timeoutUs, long thresholdUs, CountDownLatch latch, Semaphore rateSemaphore, long[] baseTime, boolean logTimeout, boolean logMax, boolean logThreshold, boolean asContent) {
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
		this.asContent = asContent;

		this.collection = collection;
		if(this.collection == null) {
			ClusterOptions options = ClusterOptions.clusterOptions(username, password);
			cluster = Cluster.connect(cbUrl, options);
			bucket = cluster.bucket(bucketname);
			collection = bucket.defaultCollection();
			bucket.waitUntilReady(Duration.ofSeconds(10));
		}
		GetResult r = collection.get(keys[0]);
		if(asContent)
			r.contentAsObject();
	}

	public void run() {
		long timeOffset=0;
		try {
			endTime = System.currentTimeMillis() + runSeconds * 1000;
			GetOptions options = GetOptions.getOptions().timeout(Duration.ofNanos(timeoutUs*1000));
			maxRecording = new Recording();
			recordings.put("timeouts", new LinkedList<Recording>()); // linked list is cheaper to extend than ArrayList
			recordings.put("thresholds", new LinkedList<Recording>()); // linked list is cheaper to extend than ArrayList
			count=0;
			sum=0;
			while (System.currentTimeMillis() < endTime) {
				if( rateSemaphore != null) {
					try {
						rateSemaphore.acquire();
					} catch (InterruptedException e) {
						break;
					}
				}
				long t0 = System.nanoTime();
				timeOffset = System.currentTimeMillis() - baseTime[0];
				boolean timeoutOccurred=false;
				try {
					GetResult r = collection.get(keys[0], options);
					if(asContent)
						r.contentAsObject();
				} catch (UnambiguousTimeoutException e) {
					timeoutOccurred=true;
				}
				long rTime = System.nanoTime() - t0;
				sum+=rTime;
				if (rTime > maxRecording.value) {
					maxRecording = new Recording(getName(), "mx", count, rTime, timeOffset);
					List<Recording> l = new LinkedList();
					l.add(maxRecording);
					recordings.put("max", l);
					if(logMax)System.out.println(maxRecording);
				}
				if(timeoutOccurred){
					Recording timeout=new Recording(getName(), "TO", count, rTime, timeOffset);
					recordings.get("timeouts").add(timeout);
					if(logTimeout) System.out.println(timeout);
				}else if (rTime > thresholdUs*1000) { // if already recorded timeout, don't also record threshold
					Recording threshold  = new Recording(getName(), rTime > timeoutUs*1000 ? "TH" : "th" , count, rTime, timeOffset);
					recordings.get("thresholds").add(threshold);
					if(logThreshold)System.out.println(threshold);
				}
				count++;
			}
		} finally {
			if(count > 0) {
				recordings.put("average", new LinkedList<Recording>());
				recordings.get("average").add(new Recording(getName(), "avg", count, sum / count, 999999999));
			}
			latch.countDown();
		}
	}

}
