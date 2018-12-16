package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.generator.UniformGenerator;

import java.util.HashMap;
import java.util.Map;

public class ConsistencyThreadState {
  private int threadId;
  private boolean isReader = true;
  private boolean isWriter = false;
  private long versionLimit;

  private Map<String, Long> taskList;
  private UniformGenerator nextTaskGenerator;

  public ConsistencyThreadState(int threadId, int numObjects, long versionStart, long versionLimit) {
    this.threadId = threadId;
    this.taskList = new HashMap<>();
    this.versionLimit = versionLimit;

    for (int i = 0; i < numObjects; i++)
      taskList.put(String.valueOf(i + 1), versionStart);

    this.nextTaskGenerator = new UniformGenerator(taskList.keySet());
  }

  public Map<String, Long> getTaskList() {
    return taskList;
  }

  public UniformGenerator getNextTaskGenerator() {
    return nextTaskGenerator;
  }

  public Long getNextObjectVersion(String key) {
    Long version = taskList.get(key);
    if (version == null)
      return null;

    long nextVersion = version + 1;

    assert nextVersion > versionLimit;

    return nextVersion;
  }

  public void acknowledgeUpdate(String key) {
    Long version = taskList.get(key);
    taskList.put(key, version + 1);

    if (++version > versionLimit) {
      taskList.remove(key);
      this.nextTaskGenerator = new UniformGenerator(taskList.keySet());
    }

  }

  public void acknowledgeRead(String key, long version) {
    if (version >= versionLimit) {
      taskList.remove(key);
      this.nextTaskGenerator = new UniformGenerator(taskList.keySet());
    }
    else
      taskList.put(key, version);
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


}
