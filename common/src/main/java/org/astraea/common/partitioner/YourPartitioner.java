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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {

  private final Map<Integer, AtomicLong> partitionUsageMap = new ConcurrentHashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    if (partitions.isEmpty()) return -1;

    // 更新每個分區的使用狀態
    updatePartitionUsage(partitions);

    // 計算每個分區的平均空間使用率
    double averageUsage = calculateAverageUsage();
    double aad = calculateAverageAbsoluteDeviation(averageUsage);

    // 選擇空間使用率最低的分區進行分配
    PartitionInfo targetPartition =
        partitions.stream()
            .min(Comparator.comparingDouble(this::getPartitionSpaceUsage))
            .orElse(partitions.get(0)); // 若無法比較則回退至第一分區

    // 更新選擇分區的使用計數
    partitionUsageMap.get(targetPartition.partition()).incrementAndGet();
    return targetPartition.partition();
  }

  @Override
  public void close() {
    partitionUsageMap.clear();
  }

  private void updatePartitionUsage(List<PartitionInfo> partitions) {
    // 初始化或更新分區的空間使用統計
    for (PartitionInfo partition : partitions) {
      partitionUsageMap.putIfAbsent(partition.partition(), new AtomicLong(0));
    }
  }

  private double getPartitionSpaceUsage(PartitionInfo partition) {
    // 根據分區的當前數據量，返回相對空間使用率
    // 在真實情境中，可能需要從監控系統或資料庫查詢空間使用率數據
    return partitionUsageMap.get(partition.partition()).doubleValue();
  }

  private double calculateAverageUsage() {
    // 計算所有分區的平均空間使用率
    return partitionUsageMap.values().stream()
        .mapToDouble(AtomicLong::doubleValue)
        .average()
        .orElse(0.0);
  }

  private double calculateAverageAbsoluteDeviation(double average) {
    // 計算所有分區的 average absolute deviation (AAD)
    return partitionUsageMap.values().stream()
        .mapToDouble(usage -> Math.abs(usage.doubleValue() - average))
        .average()
        .orElse(0.0);
  }
}
