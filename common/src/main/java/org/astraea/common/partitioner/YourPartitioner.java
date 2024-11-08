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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    partitions.stream()
        .map(PartitionInfo::leader)
        .forEach(nodeId -> nodeSpaceUtilization.put(nodeId, 0.0));
    partitions.forEach(
        partitionInfo -> {
          String partition = partitionInfo.topic() + partitionInfo.partition();
          partitionSpaceUtilization.putIfAbsent(
              partitionInfo.topic() + partitionInfo.partition(), 0.0);
          nodeToPartitions
              .computeIfAbsent(partitionInfo.leader().id(), k -> List.of())
              .add(partition);
        });
    // 1. calculate this data's space utilization
    double dataSpaceUtilization = calculateDataSpaceUtilization(keyBytes, valueBytes);
    // 2. get nodes which have the lowest space utilization
    Node selectedNode =
        nodeSpaceUtilization.entrySet().stream().min(Map.Entry.comparingByValue()).get().getKey();
    // 3. assign partition based on filtered nodes for balanced space utilization
    String selectedPartition =
        nodeToPartitions.get(selectedNode.id()).stream()
            .filter(partition -> partitionSpaceUtilization.get(partition) == 0.0)
            .findFirst()
            .orElseThrow();
    int selectedPartitions = selectedPartition.charAt(topic.length() - 1);
    // 4. update space utilization for the selected node
    nodeSpaceUtilization.put(
        selectedNode, nodeSpaceUtilization.get(selectedNode) + dataSpaceUtilization);
    partitionSpaceUtilization.put(
        topic + selectedPartitions,
        partitionSpaceUtilization.get(topic + selectedPartitions) + dataSpaceUtilization);
    return selectedPartitions;
  }

  private double calculateDataSpaceUtilization(byte[] keyBytes, byte[] valueBytes) {
    return keyBytes.length + valueBytes.length;
  }

  @Override
  public void close() {
    // Clean up resources if necessary
  }
}
