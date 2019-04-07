/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

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
  public static final String TABLENAME_PROPERTY_DEFAULT = "teste";

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

  /**
   * The name of the property for the field length distribution. Options are "uniform", "zipfian"
   * (favouring short records), "constant", and "histogram".
   * <p>
   * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
   * fieldlength property. If "histogram", then the histogram will be read from the filename
   * specified in the "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  /**
   * Generator object that produces field lengths.  The value of this depends on the properties that
   * start with "FIELD_LENGTH_".
   */
  protected NumberGenerator fieldlengthgenerator;

  protected boolean readallfields;

  /**
   * The name of the property for deciding whether to check all returned
   * data against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

  /**
   * The default value for the dataintegrity property.
   */
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;

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
   * Field name prefix.
   */
  public static final String FIELD_NAME_PREFIX = "fieldnameprefix";

  /**
   * Default value of the field name prefix.
   */
  public static final String FIELD_NAME_PREFIX_DEFAULT = "field";

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
  protected boolean orderedinserts;
  protected long fieldcount;
  protected long recordcount;
  protected int zeropadding;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;

  private Measurements measurements = Measurements.getMeasurements();

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

    dataintegrity = Boolean.parseBoolean(
        p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
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
    int numOfRetries = 0;
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
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
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

    Status status = db.read(table, keyHash, fields, cells);
    long timestamp = System.nanoTime();
    if (null != status && status.isOk()) {

      String version = cells.get(VERSION_KEY_FIELD).toString();

      if (version == null) {
        return;
      }

      threadObject.acknowledgeRead(key, Long.parseLong(version));

      System.out.println(
          "reader_id:" + threadObject.getThreadId() +
              ", key:" + keyHash +
              ", timestamp:" + timestamp +
              ", version:" + version);
    }
/*
    if (dataintegrity) {
      verifyRow(key, cells);
    }
*/
  }

  private String getKeyHash(String key) {
    byte[] bytesOfMessage = key.getBytes(UTF_8);

    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    assert md != null;
    byte[] byteArray = md.digest(bytesOfMessage);
    return Base64.getEncoder().encodeToString(byteArray);
  }

  public void doTransactionUpdate(DB db, Object threadstate) {
    ConsistencyThreadState threadObject = (ConsistencyThreadState) threadstate;

    String keyToInsert = threadObject.getNextKey();
    Long nextVersion = threadObject.getNextObjectVersion(keyToInsert);

    doInsert(db, threadstate, keyToInsert, nextVersion);
  }

  /**
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY".
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected.
   */
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    /*
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
    */
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
