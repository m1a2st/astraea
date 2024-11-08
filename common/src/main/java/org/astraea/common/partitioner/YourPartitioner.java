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
import org.apache.kafka.common.PartitionInfo;

/**
 * 目標：平衡節點 and partition 的空間使用率 次要目標：低延遲和高吞吐 空間使用率公式：average absolute deviation 考慮情境：大部分的 partitions
 * 在某個節點？ 突然跑出來的節點 or partition？ 某些節點 or partition 本來就有資料？
 */
public class YourPartitioner implements Partitioner {

  private Map<Integer, Integer> nodeToUsed = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    partitions.forEach(
        partitionInfo -> {
          int id = partitionInfo.leader().id();
          nodeToUsed.putIfAbsent(id, 0);
        });
    int min = nodeToUsed.values().stream().min(Integer::compareTo).get();
    return nodeToUsed.entrySet().stream()
        .filter(entry -> entry.getValue() == min)
        .map(Map.Entry::getKey)
        .findFirst()
        .get();
  }

  @Override
  public void close() {
    // Clean up resources if necessary
  }
}
