package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.generator.UniformGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ConsistencyThreadState {
  private int threadId;
  private boolean isReader = true;
  private boolean isWriter = false;

  private Map<ObjectKey, ObjectVersion> taskList;
  private UniformGenerator nextTaskGenerator;

  public ConsistencyThreadState(int threadId, int numObjects, long versionStart) {
    this.threadId = threadId;
    this.taskList = new HashMap<>();

    for (int i = 0; i < numObjects; i++) {
      ObjectKey objectKey = new ObjectKey(String.valueOf(i+1));
      ObjectVersion objectVersion = new ObjectVersion(versionStart);

      taskList.put(objectKey, objectVersion);
    }

    ArrayList<ObjectKey> objectKeysList = new ArrayList<>(taskList.keySet());
    ArrayList<String> keysList =
        (ArrayList<String>) objectKeysList
            .stream()
            .map(objectKey -> objectKey.key)
            .collect(Collectors.toList());
    this.nextTaskGenerator = new UniformGenerator(keysList);
  }

  public Map<ObjectKey, ObjectVersion> getTaskList() {
    return taskList;
  }

  public UniformGenerator getNextTaskGenerator() {
    return nextTaskGenerator;
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

  private class ObjectKey {
    private String key;

    public ObjectKey(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
    }
  }

  private class ObjectVersion {
    private Long version;

    public ObjectVersion(Long version) {
      this.version = version;
    }

    public Long getVersion() {
      return version;
    }

    public void setVersion(Long version) {
      this.version = version;
    }
  }
}
