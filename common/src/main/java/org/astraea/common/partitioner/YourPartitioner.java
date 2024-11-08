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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

/**
 * 目標：平衡節點 and partition 的空間使用率 次要目標：低延遲和高吞吐 空間使用率公式：average absolute deviation 考慮情境：大部分的 partitions
 * 在某個節點？ 突然跑出來的節點 or partition？ 某些節點 or partition 本來就有資料？
 */
public class YourPartitioner implements Partitioner {

  PriorityQueue<NodeWithUsed> nodes = new PriorityQueue<>(Comparator.comparing(n -> n.used));
  Map<Integer, Integer> partitionToNode = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    if (nodes.isEmpty()) {
      initializeNodeUsage(cluster.nodes());
    }

    NodeWithUsed leastUsedNode = nodes.poll();
    if (leastUsedNode == null) {
      throw new IllegalStateException("No available nodes found.");
    }

    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int partition = findPartitionOnNode(leastUsedNode.node, partitions);

    // 更新每個節點的權重, 使用平均絕對偏差來平衡節點之間的使用率
    int dataUsage = calculateDataUsage(value);
    updateNodeUsageWithBalancing(leastUsedNode, dataUsage, partitions);

    nodes.offer(leastUsedNode);
    return partition;
  }

  // 將負載平衡計算邏輯加進來
  private void updateNodeUsageWithBalancing(
      NodeWithUsed nodeWithUsed, int dataUsage, List<PartitionInfo> partitions) {
    double totalUsage = nodes.stream().mapToInt(n -> n.used).sum() + dataUsage;
    double averageUsage = totalUsage / nodes.size();

    // 以節點使用量差異作為平衡依據
    for (NodeWithUsed node : nodes) {
      node.used += Math.abs(node.used - averageUsage);
    }
    nodeWithUsed.used += dataUsage;
  }

  private void initializeNodeUsage(List<Node> clusterNodes) {
    for (Node node : clusterNodes) {
      nodes.offer(
          new NodeWithUsed(node, 0)); // Initialize usage to 0 or load from metrics if available
    }
  }

  private int findPartitionOnNode(Node node, List<PartitionInfo> partitions) {
    // Look for partitions led by this node
    for (PartitionInfo partitionInfo : partitions) {
      if (partitionInfo.leader().id() == node.id()) {
        // Map this partition to the node
        partitionToNode.put(partitionInfo.partition(), node.id());
        return partitionInfo.partition();
      }
    }
    // Default to the first partition if none are specifically led by this node
    return partitions.get(0).partition();
  }

  private void updateNodeUsage(NodeWithUsed nodeWithUsed, int dataUsage) {
    // Update the usage based on data size or some other metric
    nodeWithUsed.used += dataUsage;
  }

  private int calculateDataUsage(Object value) {
    // For example, estimate usage based on data size or a constant if data size is not available
    return value != null ? value.toString().length() : 1; // Placeholder usage metric
  }

  class NodeWithUsed {
    Node node;
    int used;

    public NodeWithUsed(Node node, int used) {
      this.node = node;
      this.used = used;
    }
  }

  @Override
  public void close() {
    // Cleanup resources if needed
  }
}
