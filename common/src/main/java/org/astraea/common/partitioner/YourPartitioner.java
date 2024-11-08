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
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * 目標：平衡節點 and partition 的空間使用率
 * 次要目標：低延遲和高吞吐
 * 空間使用率公式：average absolute deviation
 * 考慮情境：大部分的 partitions 在某個節點？
 * 突然跑出來的節點 or partition？
 * 某些節點 or partition 本來就有資料？
 */
public class YourPartitioner implements Partitioner {

    private Map<Integer, Double> nodeSpaceUtilization; // Key: Node ID, Value: Space utilization percentage
    private Map<Integer, Integer> partitionLoadMap;    // Key: Node ID, Value: Number of partitions

    @Override
    public void configure(Map<String, ?> configs) {
        // Initialize any required configurations
        nodeSpaceUtilization = new HashMap<>();
        partitionLoadMap = new HashMap<>();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int keyHash = (keyBytes == null) ? 0 : Utils.murmur2(keyBytes);
        int selectedPartition = keyHash % numPartitions;

        // Calculate space utilization and deviation
        double averageUtilization = nodeSpaceUtilization.values().stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double averageDeviation = calculateAverageAbsoluteDeviation(averageUtilization);

        // Filter out overloaded nodes
        List<Integer> candidateNodes = nodeSpaceUtilization.entrySet().stream()
                .filter(entry -> entry.getValue() <= averageUtilization + averageDeviation)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // Assign partition based on filtered nodes for balanced space utilization
        if (!candidateNodes.isEmpty()) {
            int nodeIndex = candidateNodes.get(Math.abs(keyHash) % candidateNodes.size());
            selectedPartition = getPartitionForNode(nodeIndex, partitions);
        }

        return selectedPartition;
    }

    private double calculateAverageAbsoluteDeviation(double average) {
        return nodeSpaceUtilization.values().stream()
                .mapToDouble(util -> Math.abs(util - average))
                .average()
                .orElse(0.0);
    }

    private int getPartitionForNode(int nodeId, List<PartitionInfo> partitions) {
        // Implement logic to select a specific partition for the given node based on nodeId
        // For simplicity, we assume round-robin assignment among partitions assigned to this node.
        int partitionIndex = partitionLoadMap.getOrDefault(nodeId, 0) % partitions.size();
        partitionLoadMap.put(nodeId, partitionLoadMap.get(nodeId) + 1); // Update load
        return partitions.get(partitionIndex).partition();
    }

    @Override
    public void close() {
        // Clean up resources if necessary
    }
}
