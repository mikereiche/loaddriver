package com.example.load;

public enum Execution {
  reactive,
  async,
  sync;
  public boolean isReactive(){ return this == reactive;}
  public boolean isAsync(){ return this == async;}
  public boolean isSync(){ return this == sync;}
};
