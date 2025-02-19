/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Container placement policy that choose datanodes with network topology
 * awareness, together with the space to satisfy the size constraints.
 * <p>
 * This placement policy complies with the algorithm used in HDFS. With default
 * 3 replica, two replica will be on the same rack, the third one will on a
 * different rack.
 * <p>
 * This implementation applies to network topology like "/rack/node". Don't
 * recommend to use this if the network topology has more layers.
 * <p>
 */
public final class SCMContainerPlacementRackAware
    extends SCMCommonPlacementPolicy {
  @VisibleForTesting
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerPlacementRackAware.class);
  private final NetworkTopology networkTopology;
  private boolean fallback;
  private static final int RACK_LEVEL = 1;
  private static final int MAX_RETRY = 3;
  private final SCMContainerPlacementMetrics metrics;
  // Used to check the placement policy is validated in the parent class
  private static final int REQUIRED_RACKS = 2;

  /**
   * Constructs a Container Placement with rack awareness.
   *
   * @param nodeManager Node Manager
   * @param conf Configuration
   * @param fallback Whether reducing constrains to choose a data node when
   *                 there is no node which satisfy all constrains.
   *                 Basically, false for open container placement, and true
   *                 for closed container placement.
   */
  public SCMContainerPlacementRackAware(final NodeManager nodeManager,
      final ConfigurationSource conf, final NetworkTopology networkTopology,
      final boolean fallback, final SCMContainerPlacementMetrics metrics) {
    super(nodeManager, conf);
    this.networkTopology = networkTopology;
    this.fallback = fallback;
    this.metrics = metrics;
  }

  /**
   * Called by SCM to choose datanodes.
   * There are two scenarios, one is choosing all nodes for a new pipeline.
   * Another is choosing node to meet replication requirement.
   *
   *
   * @param excludedNodes - list of the datanodes to exclude.
   * @param favoredNodes - list of nodes preferred. This is a hint to the
   *                     allocator, whether the favored nodes will be used
   *                     depends on whether the nodes meets the allocator's
   *                     requirement.
   * @param nodesRequired - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return List of datanodes.
   * @throws SCMException  SCMException
   */
  @Override
  protected List<DatanodeDetails> chooseDatanodesInternal(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
      throws SCMException {
    Preconditions.checkArgument(nodesRequired > 0);
    metrics.incrDatanodeRequestCount(nodesRequired);
    int datanodeCount = networkTopology.getNumOfLeafNode(NetConstants.ROOT);
    int excludedNodesCount = excludedNodes == null ? 0 : excludedNodes.size();
    if (datanodeCount < nodesRequired + excludedNodesCount) {
      throw new SCMException("No enough datanodes to choose. " +
          "TotalNode = " + datanodeCount +
          " RequiredNode = " + nodesRequired +
          " ExcludedNode = " + excludedNodesCount, null);
    }
    List<DatanodeDetails> mutableFavoredNodes = favoredNodes;
    // sanity check of favoredNodes
    if (mutableFavoredNodes != null && excludedNodes != null) {
      mutableFavoredNodes = new ArrayList<>();
      mutableFavoredNodes.addAll(favoredNodes);
      mutableFavoredNodes.removeAll(excludedNodes);
    }
    int favoredNodeNum = mutableFavoredNodes == null ? 0 :
        mutableFavoredNodes.size();

    List<DatanodeDetails> chosenNodes = new ArrayList<>();
    int favorIndex = 0;
    if (excludedNodes == null || excludedNodes.isEmpty()) {
      // choose all nodes for a new pipeline case
      // choose first datanode from scope ROOT or from favoredNodes if not null
      DatanodeDetails favoredNode = favoredNodeNum > favorIndex ?
          mutableFavoredNodes.get(favorIndex) : null;
      DatanodeDetails firstNode;
      if (favoredNode != null) {
        firstNode = favoredNode;
        favorIndex++;
      } else {
        firstNode = chooseNode(null, null, metadataSizeRequired,
            dataSizeRequired);
      }
      chosenNodes.add(firstNode);
      nodesRequired--;
      if (nodesRequired == 0) {
        return Arrays.asList(chosenNodes.toArray(new DatanodeDetails[0]));
      }

      // choose second datanode on the same rack as first one
      favoredNode = favoredNodeNum > favorIndex ?
          mutableFavoredNodes.get(favorIndex) : null;
      DatanodeDetails secondNode;
      if (favoredNode != null &&
          networkTopology.isSameParent(firstNode, favoredNode)) {
        secondNode = favoredNode;
        favorIndex++;
      } else {
        secondNode = chooseNode(chosenNodes, Arrays.asList(firstNode),
            metadataSizeRequired, dataSizeRequired);
      }
      chosenNodes.add(secondNode);
      nodesRequired--;
      if (nodesRequired == 0) {
        return Arrays.asList(chosenNodes.toArray(new DatanodeDetails[0]));
      }

      // choose remaining datanodes on different rack as first and second
      return chooseNodes(null, chosenNodes, mutableFavoredNodes,
          favorIndex, nodesRequired, metadataSizeRequired, dataSizeRequired);
    } else {
      List<DatanodeDetails> mutableExcludedNodes = new ArrayList<>();
      mutableExcludedNodes.addAll(excludedNodes);
      // choose node to meet replication requirement
      // case 1: one excluded node, choose one on the same rack as the excluded
      // node, choose others on different racks.
      DatanodeDetails favoredNode;
      if (excludedNodes.size() == 1) {
        favoredNode = favoredNodeNum > favorIndex ?
            mutableFavoredNodes.get(favorIndex) : null;
        DatanodeDetails firstNode;
        if (favoredNode != null &&
            networkTopology.isSameParent(excludedNodes.get(0), favoredNode)) {
          firstNode = favoredNode;
          favorIndex++;
        } else {
          firstNode = chooseNode(mutableExcludedNodes, excludedNodes,
              metadataSizeRequired, dataSizeRequired);
        }
        chosenNodes.add(firstNode);
        nodesRequired--;
        if (nodesRequired == 0) {
          return Arrays.asList(chosenNodes.toArray(new DatanodeDetails[0]));
        }
        // choose remaining nodes on different racks
        return chooseNodes(null, chosenNodes, mutableFavoredNodes, favorIndex,
            nodesRequired, metadataSizeRequired, dataSizeRequired);
      }
      // case 2: two or more excluded nodes, if these two nodes are
      // in the same rack, then choose nodes on different racks, otherwise,
      // choose one on the same rack as one of excluded nodes, remaining chosen
      // are on different racks.
      for (int i = 0; i < excludedNodesCount; i++) {
        for (int j = i + 1; j < excludedNodesCount; j++) {
          if (networkTopology.isSameParent(
              excludedNodes.get(i), excludedNodes.get(j))) {
            // choose remaining nodes on different racks
            return chooseNodes(mutableExcludedNodes, chosenNodes,
                mutableFavoredNodes, favorIndex, nodesRequired,
                metadataSizeRequired, dataSizeRequired);
          }
        }
      }
      // choose one data on the same rack with one excluded node
      favoredNode = favoredNodeNum > favorIndex ?
          mutableFavoredNodes.get(favorIndex) : null;
      DatanodeDetails secondNode;
      if (favoredNode != null && networkTopology.isSameParent(
          mutableExcludedNodes.get(0), favoredNode)) {
        secondNode = favoredNode;
        favorIndex++;
      } else {
        secondNode =
            chooseNode(chosenNodes, mutableExcludedNodes,
                metadataSizeRequired, dataSizeRequired);
      }
      chosenNodes.add(secondNode);
      mutableExcludedNodes.add(secondNode);
      nodesRequired--;
      if (nodesRequired == 0) {
        return Arrays.asList(chosenNodes.toArray(new DatanodeDetails[0]));
      }
      // choose remaining nodes on different racks
      return chooseNodes(mutableExcludedNodes, chosenNodes, mutableFavoredNodes,
          favorIndex, nodesRequired, metadataSizeRequired, dataSizeRequired);
    }
  }

  @Override
  public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
    return null;
  }

  /**
   * Choose a datanode which meets the requirements. If there is no node which
   * meets all the requirements, there is fallback chosen process depending on
   * whether fallback is allowed when this class is instantiated.
   *
   *
   * @param excludedNodes - list of the datanodes to excluded. Can be null.
   * @param affinityNodes - the chosen nodes should be on the same rack as
   *                    affinityNodes. Can be null.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return List of chosen datanodes.
   * @throws SCMException  SCMException
   */
  private DatanodeDetails chooseNode(List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> affinityNodes, long metadataSizeRequired,
      long dataSizeRequired) throws SCMException {
    int ancestorGen = RACK_LEVEL;
    int maxRetry = MAX_RETRY;
    List<String> excludedNodesForCapacity = null;
    boolean isFallbacked = false;
    while (true) {
      metrics.incrDatanodeChooseAttemptCount();
      DatanodeDetails node = null;
      if (affinityNodes != null) {
        for (Node affinityNode : affinityNodes) {
          node = (DatanodeDetails)networkTopology.chooseRandom(
              NetConstants.ROOT, excludedNodesForCapacity, excludedNodes,
              affinityNode, ancestorGen);
          if (node != null) {
            break;
          }
        }
      } else {
        node = (DatanodeDetails)networkTopology.chooseRandom(NetConstants.ROOT,
            excludedNodesForCapacity, excludedNodes, null, ancestorGen);
      }

      if (node == null) {
        // cannot find the node which meets all constrains
        LOG.warn("Failed to find the datanode for container. excludedNodes:" +
            (excludedNodes == null ? "" : excludedNodes.toString()) +
            ", affinityNode:" +
            (affinityNodes == null ? "" : affinityNodes.stream()
                .map(Object::toString).collect(Collectors.joining(", "))));
        if (fallback) {
          isFallbacked = true;
          // fallback, don't consider the affinity node
          if (affinityNodes != null) {
            affinityNodes = null;
            continue;
          }
          // fallback, don't consider cross rack
          if (ancestorGen == RACK_LEVEL) {
            ancestorGen--;
            continue;
          }
        }
        // there is no constrains to reduce or fallback is true
        throw new SCMException("No satisfied datanode to meet the" +
            " excludedNodes and affinityNode constrains.", null);
      }

      if (isValidNode(node, metadataSizeRequired, dataSizeRequired)) {
        metrics.incrDatanodeChooseSuccessCount();
        if (isFallbacked) {
          metrics.incrDatanodeChooseFallbackCount();
        }
        return node;
      }

      maxRetry--;
      if (maxRetry == 0) {
        // avoid the infinite loop
        String errMsg = "No satisfied datanode to meet the space constrains. "
            + "metadatadata size required: " + metadataSizeRequired +
            " data size required: " + dataSizeRequired;
        LOG.info(errMsg);
        throw new SCMException(errMsg, null);
      }
      if (excludedNodesForCapacity == null) {
        excludedNodesForCapacity = new ArrayList<>();
      }
      excludedNodesForCapacity.add(node.getNetworkFullPath());
    }
  }

  /**
   * Choose a batch of datanodes on different rack than excludedNodes or
   * chosenNodes.
   *
   *
   * @param excludedNodes - list of the datanodes to excluded. Can be null.
   * @param chosenNodes - list of nodes already chosen. These nodes should also
   *                    be excluded. Cannot be null.
   * @param favoredNodes - list of favoredNodes. It's a hint. Whether the nodes
   *                     are chosen depends on whether they meet the constrains.
   *                     Can be null.
   * @param favorIndex - the node index of favoredNodes which is not chosen yet.
   * @param nodesRequired - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return List of chosen datanodes.
   * @throws SCMException  SCMException
   */
  private List<DatanodeDetails> chooseNodes(List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> chosenNodes, List<DatanodeDetails> favoredNodes,
      int favorIndex, int nodesRequired, long metadataSizeRequired,
      long dataSizeRequired) throws SCMException {
    Preconditions.checkArgument(chosenNodes != null);
    List<DatanodeDetails> excludedNodeList = excludedNodes != null ?
        excludedNodes : chosenNodes;
    int favoredNodeNum = favoredNodes == null ? 0 : favoredNodes.size();
    while (true) {
      DatanodeDetails favoredNode = favoredNodeNum > favorIndex ?
          favoredNodes.get(favorIndex) : null;
      DatanodeDetails chosenNode;
      if (favoredNode != null && networkTopology.isSameParent(
          excludedNodeList.get(excludedNodeList.size() - 1), favoredNode)) {
        chosenNode = favoredNode;
        favorIndex++;
      } else {
        chosenNode = chooseNode(excludedNodeList, null,
            metadataSizeRequired, dataSizeRequired);
      }
      excludedNodeList.add(chosenNode);
      if (excludedNodeList != chosenNodes) {
        chosenNodes.add(chosenNode);
      }
      nodesRequired--;
      if (nodesRequired == 0) {
        return Arrays.asList(chosenNodes.toArray(new DatanodeDetails[0]));
      }
    }
  }

  @Override
  protected int getRequiredRackCount(int numReplicas) {
    return REQUIRED_RACKS;
  }
}
