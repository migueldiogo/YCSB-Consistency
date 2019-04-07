package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsistencyThreadState {
  private int threadId;
  private boolean isReader = true;
  private boolean isWriter = false;
  private long versionLimit;

  private Queue<String> keyTaskQueue;
  private Map<String, Long> keyRegistry;

  public ConsistencyThreadState(int threadId, int numObjects, long versionStart, long versionLimit) {
    this.threadId = threadId;
    this.keyRegistry = new HashMap<>();
    this.versionLimit = versionLimit;
    this.keyTaskQueue = new LinkedBlockingQueue<>(numObjects);

    for (int i = 0; i < numObjects; i++) {
      keyRegistry.put(String.valueOf(i + 1), versionStart);
      keyTaskQueue.add("" + (i + 1));
    }
  }

  public Map<String, Long> getKeyRegistry() {
    return keyRegistry;
  }

  public String getNextKey() {
    return keyTaskQueue.peek();
  }

  public Long getCurrentObjectVersion(String key) {
    return keyRegistry.get(key);
  }

  public Long setCurrentObjectVersion(String key, long version) {
    return keyRegistry.put(key, version);
  }

  public Long getNextObjectVersion(String key) {
    Long version = getCurrentObjectVersion(key);
    if (version == null)
      return null;

    long nextVersion = version + 1;

    assert nextVersion > versionLimit;

    return nextVersion;
  }

  public void acknowledgeUpdate(String key) {
    Long version = getCurrentObjectVersion(key);

    long nextVersion = version + 1;
    setCurrentObjectVersion(key, nextVersion);

    if (nextVersion >= versionLimit) {
      this.keyTaskQueue.poll();
    }
  }

  public void acknowledgeRead(String key, long version) {
    if (version == versionLimit) {
      this.keyTaskQueue.poll();
    }
  }

  public boolean isReader() {
    return isReader;
  }

  public void setReader(boolean reader) {
    isReader = reader;
  }

  public boolean isWriter() {
    return isWriter;
  }

  public void setWriter(boolean writer) {
    isWriter = writer;
  }

  public int getThreadId() {
    return threadId;
  }

  public void setKeyRegistry(Map<String, Long> keyRegistry) {
    this.keyRegistry = keyRegistry;
  }
}
