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
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {

  private Map<Integer, PriorityQueue<NodeWithUsed>> partitionToNodesUsage = new HashMap<>();
  private Map<Node, Integer> nodeToUsage = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    // Step 1: 初始化或更新 partition-to-nodes usage 表
    if (partitionToNodesUsage.isEmpty()) {
      initializePartitionNodeMapping(cluster, topic);
    } else if (partitionToNodesUsage.size() != cluster.partitionsForTopic(topic).size()) {
      updatePartitionNodeMapping(cluster, topic);
    }

    // Step 2: 為指定主題找到負載最低的分區
    int selectedPartition = -1;
    int leastUsage = Integer.MAX_VALUE;

    for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
      PriorityQueue<NodeWithUsed> nodesUsageQueue =
          partitionToNodesUsage.get(partitionInfo.partition());
      if (nodesUsageQueue == null || nodesUsageQueue.isEmpty()) {
        continue;
      }

      NodeWithUsed leastUsedNode = nodesUsageQueue.peek();
      if (leastUsedNode != null && leastUsedNode.usage < leastUsage) {
        leastUsage = leastUsedNode.usage;
        selectedPartition = partitionInfo.partition();
      }
    }

    // Step 3: 更新選中分區的負載
    if (selectedPartition != -1) {
      PriorityQueue<NodeWithUsed> selectedNodesQueue = partitionToNodesUsage.get(selectedPartition);
      NodeWithUsed leastUsedNode = selectedNodesQueue.poll();
      if (leastUsedNode != null) {
        leastUsedNode.usage += calculateDataUsage(value);
        selectedNodesQueue.offer(leastUsedNode);
      }
    }

    return selectedPartition;
  }

  private void initializePartitionNodeMapping(Cluster cluster, String topic) {
    for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
      PriorityQueue<NodeWithUsed> nodesQueue =
          new PriorityQueue<>(Comparator.comparingInt(n -> n.usage));
      for (Node replica : partitionInfo.replicas()) {
        nodesQueue.offer(new NodeWithUsed(replica, nodeToUsage.getOrDefault(replica, 0)));
      }
      partitionToNodesUsage.put(partitionInfo.partition(), nodesQueue);
    }
  }

  private void updatePartitionNodeMapping(Cluster cluster, String topic) {
    for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
      if (!partitionToNodesUsage.containsKey(partitionInfo.partition())) {
        PriorityQueue<NodeWithUsed> nodesQueue =
            new PriorityQueue<>(Comparator.comparingInt(n -> n.usage));
        for (Node replica : partitionInfo.replicas()) {
          nodesQueue.offer(new NodeWithUsed(replica, nodeToUsage.getOrDefault(replica, 0)));
        }
        partitionToNodesUsage.put(partitionInfo.partition(), nodesQueue);
      }
    }
  }

  private int calculateDataUsage(Object value) {
    return value != null ? value.toString().length() : 1; // 計算使用量的佔位符
  }

  @Override
  public void close() {}

  class NodeWithUsed {
    Node node;
    int usage;

    public NodeWithUsed(Node node, int usage) {
      this.node = node;
      this.usage = usage;
    }
  }
}
