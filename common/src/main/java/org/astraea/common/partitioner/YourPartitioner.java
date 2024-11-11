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
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

/**
 * 目標：平衡節點 and partition的空間使用率
 *
 * <p>次要目標：低延遲和高吞吐
 *
 * <p>空間使用率公式：average absolute deviation
 *
 * <p>考慮情境：大部分的 partitions 在某個節點？突然跑出來的節點 or partition？某些節點 or partition 本來就有資料？
 */
public class YourPartitioner implements Partitioner {

  private final Set<Integer> nodes = new HashSet<>();
  private final PriorityQueue<NodeUsage> nodeUsage =
      new PriorityQueue<>(Comparator.comparingInt(n -> n.usage));

  @Override
  public void configure(Map<String, ?> configs) {
    // Any specific configuration setup can be added here.
  }

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    // Update nodes and node usage information
    updateNodesAndUsage(cluster);

    // Calculate data size of the message
    int dataSize = calculateDataSize(valueBytes);

    // Select the node with minimum usage and assign the partition
    NodeUsage leastUsedNode = nodeUsage.poll();
    if (leastUsedNode != null) {
      leastUsedNode.usage += dataSize; // Update usage for the selected node
      nodeUsage.offer(leastUsedNode); // Reinsert to maintain order in the priority queue
      return findPartitionForNode(topic, cluster, leastUsedNode.nodeId);
    }

    // Default partition assignment if node information is unavailable
    return 0;
  }

  private void updateNodesAndUsage(Cluster cluster) {
    // Refresh nodes and usage if cluster topology has changed
    for (PartitionInfo partition : cluster.partitionsForTopic("your-topic")) {
      nodes.add(partition.leader().id()); // Track unique node IDs
      nodeUsage.offer(new NodeUsage(partition.leader().id(), 0)); // Initialize with zero usage
    }
  }

  private int findPartitionForNode(String topic, Cluster cluster, int nodeId) {
    // Find a partition for a given node ID to ensure distribution across partitions
    for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
      if (partition.leader().id() == nodeId) {
        return partition.partition();
      }
    }
    // Fallback if no specific partition found for nodeId
    return 0;
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
  public void close() {
    // Cleanup resources if needed
  }
}
