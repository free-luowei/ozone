/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;

/**
 * Batch operation implementation for rocks db.
 */
public class RDBBatchOperation implements BatchOperation {

  private final WriteBatch writeBatch;

  public RDBBatchOperation() {
    writeBatch = new WriteBatch();
  }

  public RDBBatchOperation(WriteBatch writeBatch) {
    this.writeBatch = writeBatch;
  }

  public void commit(RocksDatabase db) throws IOException {
    db.batchWrite(writeBatch);
  }

  public void commit(RocksDatabase db, WriteOptions writeOptions)
      throws IOException {
    db.batchWrite(writeBatch, writeOptions);
  }

  @Override
  public void close() {
    writeBatch.close();
  }

  public void delete(ColumnFamily family, byte[] key) throws IOException {
    family.batchDelete(writeBatch, key);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    family.batchPut(writeBatch, key, value);
  }
}
