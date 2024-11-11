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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {

  private final Set<Integer> nodes = new HashSet<>();
  private final PriorityQueue<NodeUsage> nodeUsage =
      new PriorityQueue<>(Comparator.comparingInt(n -> n.usage));
  private final Map<Integer, List<PartitionInfo>> nodeToPartitions = new HashMap<>();

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

    assignPartitionsToNodes(cluster.availablePartitionsForTopic(topic));
    return 100;
    //    return selectPartitionForNode(valueBytes);
  }

  private void assignPartitionsToNodes(List<PartitionInfo> partitions) {
    for (PartitionInfo partitionInfo : partitions) {
      int nodeId = findPartitionToNode(partitionInfo);
      nodeToPartitions.get(nodeId).add(partitionInfo);
    }
  }

  private int selectPartitionForNode(byte[] valueBytes) {
    Queue<NodeUsage> unusedNodes = new PriorityQueue<>(nodeUsage); // 使用副本，保持 nodeUsage 原狀
    int dataSize = calculateDataSize(valueBytes);

    while (!unusedNodes.isEmpty()) {
      NodeUsage leastUsedNode = unusedNodes.poll(); // 提取最小負載節點
      int leastUsedNodeId = leastUsedNode.nodeId;
      List<PartitionInfo> assignedPartitions = nodeToPartitions.get(leastUsedNodeId);

      // 檢查該節點是否有可用分區
      if (!assignedPartitions.isEmpty()) {
        PartitionInfo partition = assignedPartitions.remove(0); // 選擇分區
        int sendPartition = partition.partition();

        // 更新原始 nodeUsage 中的該節點負載
        nodeUsage.remove(leastUsedNode); // 移除並更新使用率
        leastUsedNode.usage += dataSize;
        nodeUsage.offer(leastUsedNode); // 將更新後的節點重新放回原始隊列

        return sendPartition;
      }
    }
    // 當所有節點無可用分區時，返回預設分區（可根據需求設定）
    return 0;
  }

  private int findPartitionToNode(PartitionInfo partitionInfo) {
    Set<Integer> replicaNodes = new HashSet<>();
    replicaNodes.add(partitionInfo.leader().id());
    Arrays.stream(partitionInfo.inSyncReplicas())
        .forEach(replica -> replicaNodes.add(replica.id()));
    Arrays.stream(partitionInfo.replicas()).forEach(replica -> replicaNodes.add(replica.id()));

    Set<Integer> nonReplicaNodes = new HashSet<>(nodes);
    nonReplicaNodes.removeAll(replicaNodes);

    return nonReplicaNodes.isEmpty() ? nodes.iterator().next() : nonReplicaNodes.iterator().next();
  }

  private void initializeNodeUsage(Cluster cluster) {
    for (Node node : cluster.nodes()) {
      nodeUsage.add(new NodeUsage(node.id(), 0));
      nodeToPartitions.put(node.id(), new ArrayList<>());
      nodes.add(node.id());
    }
  }

  private void updateNodeUsage(Cluster cluster) {
    for (Node node : cluster.nodes()) {
      if (!nodeToPartitions.containsKey(node.id())) {
        nodeUsage.add(new NodeUsage(node.id(), 0));
        nodeToPartitions.put(node.id(), new ArrayList<>());
        nodes.add(node.id());
      }
    }
  }

  private int calculateDataSize(byte[] data) {
    return data == null ? 0 : data.length;
  }

  private static class NodeUsage {
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
