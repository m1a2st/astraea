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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

/**
 * 目標：平衡節點 and partition 的空間使用率 次要目標：低延遲和高吞吐 空間使用率公式：average absolute deviation 考慮情境：大部分的 partitions
 * 在某個節點？ 突然跑出來的節點 or partition？ 某些節點 or partition 本來就有資料？
 */
public class YourPartitioner implements Partitioner {

  // Key: Node ID, Value: Space utilization percentage
  private Map<Node, Double> nodeSpaceUtilization;
  private Map<String, Double> partitionSpaceUtilization;
  private Map<Integer, List<String>> nodeToPartitions;

  @Override
  public void configure(Map<String, ?> configs) {
    // Initialize any required configurations
    nodeSpaceUtilization = new HashMap<>();
    partitionSpaceUtilization = new HashMap<>();
    nodeToPartitions = new HashMap<>();
  }

  /**
   * Step
   *
   * <p>1. calculate this data's space utilization
   *
   * <p>2. get nodes which have the lowest space utilization
   *
   * <p>3. assign partition based on filtered nodes for balanced space utilization
   *
   * <p>4. update space utilization for the selected node 6. return partition
   */
  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    // Step 1: Calculate the space utilization for the incoming data
    double dataSpaceUtilization = calculateDataSpaceUtilization(value);

    // Step 2: Identify nodes with the lowest space utilization
    List<Node> candidateNodes =
        nodeSpaceUtilization.entrySet().stream()
            .sorted(Map.Entry.comparingByValue()) // Sort nodes by utilization in ascending order
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    Node selectedNode = null;

    // Step 3: Assign partition based on filtered nodes for balanced space utilization
    for (Node node : candidateNodes) {
      List<String> partitionsOnNode = nodeToPartitions.getOrDefault(node.id(), new ArrayList<>());
      if (!partitionsOnNode.isEmpty()) {
        selectedNode = node;
        break;
      }
    }

    if (selectedNode == null) {
      // Fallback to default partitioning if no suitable node is found
      selectedNode = cluster.availablePartitionsForTopic(topic).get(0).leader();
    }

    // Step 4: Select a partition on the chosen node
    int partition = selectPartitionForNode(selectedNode, cluster.partitionsForTopic(topic));

    // Step 5: Update the space utilization for the selected node and partition
    updateSpaceUtilization(selectedNode, partition, dataSpaceUtilization);

    return partition;
  }

  private double calculateDataSpaceUtilization(Object value) {
    // Estimate the space utilization for this data based on its size or other criteria
    // For example, you could use value's size in bytes
    return (value != null) ? value.toString().length() * 0.01 : 0.0; // Example calculation
  }

  private int selectPartitionForNode(Node node, List<PartitionInfo> partitions) {
    // Implement logic to select a specific partition for the given node based on node ID
    // Example: Choose the least loaded partition
    for (PartitionInfo partitionInfo : partitions) {
      if (partitionInfo.leader().id() == node.id()) {
        return partitionInfo.partition();
      }
    }
    return 0; // Default partition if no matching partition found
  }

  private void updateSpaceUtilization(Node node, int partition, double dataSpaceUtilization) {
    // Update node's space utilization
    nodeSpaceUtilization.put(
        node, nodeSpaceUtilization.getOrDefault(node, 0.0) + dataSpaceUtilization);

    // Update partition space utilization
    String partitionKey = node.id() + "-" + partition;
    partitionSpaceUtilization.put(
        partitionKey,
        partitionSpaceUtilization.getOrDefault(partitionKey, 0.0) + dataSpaceUtilization);

    // Record the partition assignment
    nodeToPartitions.computeIfAbsent(node.id(), k -> new ArrayList<>()).add(partitionKey);
  }

  @Override
  public void close() {
    // Cleanup resources if needed
  }
}
