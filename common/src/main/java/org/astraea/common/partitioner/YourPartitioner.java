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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {

  private final Map<Integer, AtomicLong> brokerSpaceUsageMap = new ConcurrentHashMap<>();
  private final Map<Integer, Integer> partitionToBrokerMap = new ConcurrentHashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {
    // 配置初始化，可以是外部監控系統的資料來源
  }

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    if (partitions.isEmpty()) return -1;

    // 更新分區到 broker 的映射表
    updatePartitionToBrokerMap(partitions);

    // 計算平均空間使用率
    double averageUsage = calculateAverageUsage();
    double aad = calculateAverageAbsoluteDeviation(averageUsage);

    // 選擇空間使用率最低的分區所屬的 broker
    int targetBrokerId =
        brokerSpaceUsageMap.entrySet().stream()
            .min(
                (k, v) -> {
                  double diff = v.getValue().doubleValue() - averageUsage;
                  return diff == 0 ? 0 : diff > 0 ? 1 : -1;
                })
            .map(Map.Entry::getKey)
            .orElse(partitions.get(0).leader().id());

    // 找到目標 broker 上的分區
    PartitionInfo targetPartition =
        partitions.stream()
            .filter(partition -> partitionToBrokerMap.get(partition.partition()) == targetBrokerId)
            .findFirst()
            .orElse(partitions.get(0)); // 若無法找到，回退至第一分區

    // 更新選擇分區的 broker 使用狀態
    brokerSpaceUsageMap.get(targetBrokerId).incrementAndGet();
    return targetPartition.partition();
  }

  @Override
  public void close() {
    brokerSpaceUsageMap.clear();
    partitionToBrokerMap.clear();
  }

  private void updatePartitionToBrokerMap(List<PartitionInfo> partitions) {
    for (PartitionInfo partition : partitions) {
      partitionToBrokerMap.put(partition.partition(), partition.leader().id());
      brokerSpaceUsageMap.putIfAbsent(partition.leader().id(), new AtomicLong(0));
    }
  }

  private double calculateAverageUsage() {
    return brokerSpaceUsageMap.values().stream()
        .mapToDouble(AtomicLong::doubleValue)
        .average()
        .orElse(0.0);
  }

  private double calculateAverageAbsoluteDeviation(double average) {
    return brokerSpaceUsageMap.values().stream()
        .mapToDouble(usage -> Math.abs(usage.doubleValue() - average))
        .average()
        .orElse(0.0);
  }
}
