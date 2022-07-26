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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell.keys;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketHandler;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Executes Recover Trash Keys.
 */
@Command(name = "recover-trash",
    aliases = "",
    description = "recover key from trash to bucket")
public class RecoverTrashHandler extends KeyHandler {

  @Parameters(index = "1", arity = "1..1",
      description = "Dest bucket to recover trash")
  private String destBucket;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String key = address.getKeyName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    boolean ret = bucket.recoverTrash(key, destBucket);

    // More keys were returned notify about max length
    if (ret) {
      out().println("recoverKey " + key + " from trash " +
          "success.");
    }
  }
}
