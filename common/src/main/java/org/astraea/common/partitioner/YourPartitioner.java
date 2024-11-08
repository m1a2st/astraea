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
import java.util.Objects;
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
  Map<CustomPartition, Integer> partitionToUsage = new HashMap<>();

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
    partitions.forEach(
        p -> partitionToUsage.putIfAbsent(new CustomPartition(p.topic(), p.partition()), 0));
    CustomPartition selectedPartition = findPartitionOnNode(partitions, leastUsedNode.node);

    // 更新節點與分區的使用率
    int dataUsage = calculateDataUsage(key, value);
    updateNodeUsage(leastUsedNode, dataUsage);
    updatePartitionUsage(selectedPartition, dataUsage);

    nodes.offer(leastUsedNode);
    return selectedPartition.partition;
  }

  private void updatePartitionUsage(CustomPartition selectedPartition, int dataUsage) {
    partitionToUsage.put(selectedPartition, partitionToUsage.get(selectedPartition) + dataUsage);
  }

  private void initializeNodeUsage(List<Node> clusterNodes) {
    for (Node node : clusterNodes) {
      nodes.offer(new NodeWithUsed(node, 0));
    }
  }

  private void updateNodeUsage(NodeWithUsed nodeWithUsed, int dataUsage) {
    nodeWithUsed.used += dataUsage;
  }

  private CustomPartition findPartitionOnNode(List<PartitionInfo> partitions, Node node) {
    return partitions.stream()
        .filter(p -> p.leader().equals(node))
        .map(p -> new CustomPartition(p.topic(), p.partition()))
        .min(Comparator.comparingInt(partitionToUsage::get))
        .orElseThrow(() -> new IllegalStateException("No partition found on node " + node.host()));
  }

  private int calculateDataUsage(Object key, Object value) {
    int keySize = key != null ? key.toString().length() : 1;
    int valueSize = value != null ? value.toString().length() : 1;
    return keySize + valueSize;
  }

  class NodeWithUsed {
    Node node;
    int used;

    public NodeWithUsed(Node node, int used) {
      this.node = node;
      this.used = used;
    }
  }

  class CustomPartition {
    String topic;
    int partition;

    public CustomPartition(String topic, int partition) {
      this.topic = topic;
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, partition);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      CustomPartition that = (CustomPartition) obj;
      return partition == that.partition && Objects.equals(topic, that.topic);
    }
  }

  @Override
  public void close() {
    // Cleanup resources if needed
  }
}
