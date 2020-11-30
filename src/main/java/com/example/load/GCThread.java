package com.example.load;

public class GCThread extends Thread{
	long intervalMs;
	long runSeconds;
	long startTime;

	public GCThread(long intervalMs, long runSeconds, long startTime){
		this.intervalMs = intervalMs;
		this.runSeconds = runSeconds;
		this.startTime = startTime;
	}

	@Override
	public void run(){
		long endTime = System.currentTimeMillis() + runSeconds*1000;
		long t0;
		while (System.currentTimeMillis() < endTime) {
			t0=System.currentTimeMillis();
			long timeOffset=t0 - startTime;
			System.gc();
			t0=System.currentTimeMillis()-t0;
			System.out.println("GC: "+t0+"ms time: "+timeOffset);
			try {
				Thread.sleep(intervalMs-t0);
			} catch (InterruptedException e) {
			}
		}
	}
}
