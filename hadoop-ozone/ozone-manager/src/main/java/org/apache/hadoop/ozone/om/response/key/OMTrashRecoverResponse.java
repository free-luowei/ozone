/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Response for RecoverTrash request.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE})
public class OMTrashRecoverResponse extends OmKeyResponse {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMTrashRecoverResponse.class);

  private OmKeyInfo omKeyInfo;
  private OmBucketInfo omBucketInfo;

  public OMTrashRecoverResponse(@Nullable OmKeyInfo omKeyInfo,
      @Nonnull OMResponse omResponse, OmBucketInfo omBucketInfo) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.omKeyInfo = omKeyInfo;
    this.omBucketInfo = omBucketInfo;
  }

  public OMTrashRecoverResponse(@Nullable OmKeyInfo omKeyInfo,
      @Nonnull OMResponse omResponse, BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.omKeyInfo = omKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

      /* TODO: HDDS-2425. HDDS-2426. */
    String trashKey = omMetadataManager
        .getOzoneKey(omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
    LOG.error("OMTrashRecover {}, input omKey={}", trashKey, omKeyInfo.getObjectInfo());

    RepeatedOmKeyInfo repeatedOmKeyInfo = omMetadataManager
        .getDeletedTable().get(trashKey);
    RepeatedOmKeyInfo newRepeatedOmKeyInfo = OmUtils.prepareKeyForRecover(omKeyInfo, repeatedOmKeyInfo);
    if (newRepeatedOmKeyInfo == null) {
        LOG.error("OMTrashRecover prepareKeyForRecover return null");
        return;
    }
    LOG.error("OMTrashRecover omKey={}, repeatedOmKey={}", omKeyInfo.getObjectInfo(), repeatedOmKeyInfo.getObjectInfo());

    if (newRepeatedOmKeyInfo.getOmKeyInfoList().size() > 0) {
      omMetadataManager.getDeletedTable()
        .putWithBatch(batchOperation, trashKey, newRepeatedOmKeyInfo);
    } else {
      omMetadataManager.getDeletedTable()
          .deleteWithBatch(batchOperation, trashKey);
    }
    /* TODO: trashKey should be updated to destinationBucket. */
    omMetadataManager.getKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, trashKey, omKeyInfo);
  }

}
