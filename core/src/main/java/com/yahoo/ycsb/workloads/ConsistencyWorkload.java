package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.DiscreteGenerator;

import java.util.*;

import static java.lang.Thread.sleep;

/**
 *
 *
 */
public class ConsistencyWorkload extends Workload {
  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "ycsb";

  protected String table;

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  private List<String> fieldnames;

  protected boolean readallfields;

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.5";

  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.5";

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

  /**
   * CUSTOM:
   */
  public static final String NUM_OBJECTS = "numobjects";
  public static final String NUM_OBJECTS_PROPERTY_DEFAULT = "10";
  public static final String OBJECT_VERSION_START = "objectversionstart";
  public static final String OBJECT_VERSION_START_PROPERTY_DEFAULT = "0";
  public static final String OBJECT_VERSION_LIMIT = "objectversionlimit";
  public static final String OBJECT_VERSION_LIMIT_PROPERTY_DEFAULT = "5";

  public static final String VERSION_KEY_FIELD = "version";

  private int numObjects;
  private long objectVersionStart;
  private long objectVersionLimit;

  protected DiscreteGenerator operationchooser;
  protected long fieldcount;
  protected long recordcount;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    numObjects = Integer.parseInt(p.getProperty(NUM_OBJECTS, NUM_OBJECTS_PROPERTY_DEFAULT));
    objectVersionStart = Long.parseLong(p.getProperty(OBJECT_VERSION_START, OBJECT_VERSION_START_PROPERTY_DEFAULT));
    objectVersionLimit = Long.parseLong(p.getProperty(OBJECT_VERSION_LIMIT, OBJECT_VERSION_LIMIT_PROPERTY_DEFAULT));

    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    fieldnames = new ArrayList<>();
    fieldnames.add(VERSION_KEY_FIELD);

    recordcount =
        Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }

    operationchooser = createOperationGenerator(p);

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) {
    return new ConsistencyThreadState(mythreadid, numObjects, objectVersionStart, objectVersionLimit);
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    ConsistencyThreadState threadObject = (ConsistencyThreadState) threadstate;
    if (threadObject.getThreadId() == 0) {
      for (int i = 0; i < numObjects; i++) {
        String key = String.valueOf(i + 1);
        doInsert(db, threadstate, key, objectVersionStart);
      }
    }

    return false;
  }

  public boolean doInsert(DB db, Object threadstate, String key, Long version) {
    ConsistencyThreadState threadObject = (ConsistencyThreadState) threadstate;

    String keyHash = getKeyHash(key);

    Map<String, ByteIterator> values = new HashMap<>();
    values.put(VERSION_KEY_FIELD, new StringByteIterator(version.toString()));

    Status status;
    do {
      status = db.insert(table, keyHash, values);
      long timestamp = System.nanoTime();
      if (null != status && status.isOk()) {

        threadObject.acknowledgeUpdate(key);

        System.out.println(
            "writer_id:" + threadObject.getThreadId() +
            ", key:" + keyHash +
            ", timestamp:" + timestamp +
            ", version:" + version);

        break;
      } else {
        System.out.println(
            "reader_id:" + threadObject.getThreadId() +
                ", key:" + keyHash +
                ", timestamp:" + timestamp +
                ", version: UNAVAILABLE"
        );
        try {
          sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } while (true);

    return status.isOk();
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    ConsistencyThreadState threadObject = (ConsistencyThreadState) threadstate;
    if (threadObject.getNextKey() == null)
      return false;

    String operation;

    if (threadObject.getThreadId() == 0)
      operation = "UPDATE";
    else
      operation = "READ";

    switch (operation) {
      case "READ":
        doTransactionRead(db, threadstate);
        break;
      case "UPDATE":
        doTransactionUpdate(db, threadstate);
        break;
    }

    return true;
  }

  public void doTransactionRead(DB db, Object threadstate) {
    ConsistencyThreadState threadObject = (ConsistencyThreadState) threadstate;
    String key = threadObject.getNextKey();

    String keyHash = getKeyHash(key);

    HashSet<String> fields = new HashSet<>(fieldnames);

    HashMap<String, ByteIterator> cells = new HashMap<>();

    long timestamp = System.nanoTime();
    Status status = db.read(table, keyHash, fields, cells);
    if (null != status && status.isOk()) {
      String version = cells.get(VERSION_KEY_FIELD).toString();

      if (version == null) {
        return;
      }

      threadObject.acknowledgeRead(Long.parseLong(version));

      System.out.println(
          "reader_id:" + threadObject.getThreadId() +
          ", key:" + keyHash +
          ", timestamp:" + timestamp +
          ", version:" + version);
    } else {
      System.out.println(
          "reader_id:" + threadObject.getThreadId() +
              ", key:" + keyHash +
              ", timestamp:" + timestamp +
              ", version: UNAVAILABLE"
      );
      try {
        sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private String getKeyHash(String key) {
    /*
    byte[] bytesOfMessage = key.getBytes(UTF_8);

    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
      md.update(key.getBytes(), 0, key.length());

    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    assert md != null;
    byte[] byteArray = md.digest(bytesOfMessage);

    BigInteger bi = new BigInteger(1, byteArray);
*/
    return key;
  }

  public void doTransactionUpdate(DB db, Object threadstate) {
    ConsistencyThreadState threadObject = (ConsistencyThreadState) threadstate;

    String keyToInsert = threadObject.getNextKey();
    Long nextVersion = threadObject.getNextObjectVersion(keyToInsert);

    doInsert(db, threadstate, keyToInsert, nextVersion);
  }


  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    return operationchooser;
  }
}
