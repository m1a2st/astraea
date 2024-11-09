/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.partitioner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {

  Set<Integer> nodes = new HashSet<>();
  PriorityQueue<NodeUsage> nodeUsage = new PriorityQueue<>(Comparator.comparingInt(a -> a.usage));
  Map<Integer, List<PartitionInfo>> nodeToPartitions = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    if (nodeUsage.isEmpty()) {
      initializeNodeUsage(cluster);
    } else if (nodeUsage.size() != cluster.nodes().size()) {
      updateNodeUsage(cluster);
    }

    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);

    // 分配 partitions 到節點
    partitions.forEach(
        partitionInfo -> {
          Node node = partitionInfo.leader();
          Node[] inSyncReplicas = partitionInfo.inSyncReplicas();
          Node[] replicas = partitionInfo.replicas();
          Set<Integer> replicaNodes = new HashSet<>();
          for (Node replica : replicas) {
            replicaNodes.add(replica.id());
          }
          for (Node inSyncReplica : inSyncReplicas) {
            replicaNodes.add(inSyncReplica.id());
          }
          replicaNodes.add(node.id());
          int nodeId = compareNodes(nodes, replicaNodes);
          nodeToPartitions.get(nodeId).add(partitionInfo);
        });

    // 按照節點負載分配 partition
    boolean isCompleted = false;
    int sendPartition = 0;

    while (!isCompleted) {
      NodeUsage leastUsedNode = nodeUsage.poll(); // 擷取負載最少的節點
      int leastUsedNodeId = leastUsedNode.nodeId;

      List<PartitionInfo> assignedPartitions = nodeToPartitions.get(leastUsedNodeId);

      // 根據空間使用率選擇分區，並更新負載
      if (!assignedPartitions.isEmpty()) {
        // 這裡根據需求可以調整，將 partition 分配給這個節點
        PartitionInfo partition = assignedPartitions.remove(0); // 假設只分配一個 partition
        sendPartition = partition.partition(); // 記錄分配的 partition

        // 更新該節點的使用率
        int dataSize = calculateDataSize(valueBytes);
        leastUsedNode.usage += dataSize;

        // 將節點重新放入優先隊列
        nodeUsage.offer(leastUsedNode);
        isCompleted = true; // 結束
      }
    }

    return sendPartition;
  }

  private int compareNodes(Set<Integer> nodes, Set<Integer> replicaNodes) {
    nodes.removeAll(replicaNodes);
    int nodeId = nodes.iterator().next();
    nodes.addAll(replicaNodes);
    return nodeId;
  }

  private void initializeNodeUsage(Cluster cluster) {
    for (Node node : cluster.nodes()) {
      nodeUsage.add(new NodeUsage(node.id(), 0)); // 初始化所有節點的使用量
      nodeToPartitions.put(node.id(), new ArrayList<>());
      nodes.add(node.id());
    }
  }

  private void updateNodeUsage(Cluster cluster) {
    for (Node node : cluster.nodes()) {
      if (!nodeToPartitions.containsKey(node.id())) {
        nodeUsage.add(new NodeUsage(node.id(), 0)); // 新節點的初始化
        nodeToPartitions.put(node.id(), new ArrayList<>());
        nodes.add(node.id());
      }
    }
  }

  private int calculateDataSize(byte[] data) {
    return data == null ? 0 : data.length; // 計算數據大小
  }

  class NodeUsage {
    int nodeId;
    int usage;

    NodeUsage(int nodeId, int usage) {
      this.nodeId = nodeId;
      this.usage = usage;
    }
  }

  @Override
  public void close() {}
}
