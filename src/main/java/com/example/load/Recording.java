package com.example.load;

public class Recording {
	public String threadName;
	public String name;
	public long count;
	public long value;
	public long timeOffset;

	public Recording( String threadName, String name, long count, long value, long timeOffset){
		this.threadName = threadName;
		this.name = name;
		this.count = count;
		this.value = value;
		this.timeOffset = timeOffset;
	}

	public Recording() {
	}

	long getValue(){
		return value;
	}

	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append("{ ");
		sb.append(String.valueOf(name));
		sb.append(", ");
		sb.append(threadName);
		sb.append(", count: ");
		sb.append(String.valueOf(count));
		sb.append(", value: ");
		sb.append(String.valueOf(value/1000)+ "us");
		sb.append(", time: ");
		sb.append(String.valueOf(timeOffset)+"ms");
		sb.append(" }");
		return sb.toString();
	}
}
