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
 * 目標：平衡節點 and partition的空間使用率
 *
 * <p>次要目標：低延遲和高吞吐
 *
 * <p>空間使用率公式：average absolute deviation
 *
 * <p>考慮情境：大部分的 partitions 在某個節點？突然跑出來的節點 or partition？某些節點 or partition 本來就有資料？
 */
public class YourPartitioner implements Partitioner {

  private final Map<Integer, Integer> partitionUsage = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {
    // 初始化或配置使用紀錄
  }

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    // 計算負載最小的 partition
    int minLoadPartition = getLeastLoadedPartition(topic, cluster);
    updateUsage(minLoadPartition, valueBytes);

    return minLoadPartition;
  }

  private int getLeastLoadedPartition(String topic, Cluster cluster) {
    // 遍歷 partitionUsage 找到負載最小的 partition
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int minPartition = partitions.get(0).partition();
    int minUsage = partitionUsage.getOrDefault(minPartition, 0);

    for (PartitionInfo partition : partitions) {
      int usage = partitionUsage.getOrDefault(partition.partition(), 0);
      if (usage < minUsage) {
        minUsage = usage;
        minPartition = partition.partition();
      }
    }

    return minPartition;
  }

  private void updateUsage(int partition, byte[] valueBytes) {
    int dataSize = calculateDataSize(valueBytes);
    partitionUsage.put(partition, partitionUsage.getOrDefault(partition, 0) + dataSize);
  }

  private int calculateDataSize(byte[] data) {
    return data == null ? 0 : data.length;
  }

  @Override
  public void close() {
    // 清理資源
  }
}
