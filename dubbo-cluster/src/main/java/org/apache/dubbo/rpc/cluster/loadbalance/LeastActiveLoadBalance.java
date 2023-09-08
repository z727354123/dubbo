/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 * 最少活跃调用数负载均衡，主要用于筛选活跃调用数最少的Invoker，并根据权重和数量选择调用
 * 如果只有一个Invoker，则直接选择该Invoker
 * 如果有多个Invoker且权重不相等，则根据总权重随机选择
 * 如果有多个Invoker且权重相等，则随机选择一个
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 调用器个数
        int length = invokers.size();
        // 所有调用器中的最少活动数
        int leastActive = -1;
        // 最少活动数相同的调用器的个数
        int leastCount = 0;
        // 最少活动数相同的调用器的索引
        int[] leastIndexes = new int[length];
        // 每个调用器的权重值
        int[] weights = new int[length];
        // 最少活动数的调用器中所有调用器的权重之和
        int totalWeight = 0;
        // 第一个最少活动数的调用器的权重值
        int firstWeight = 0;
        // 每个最少活动数的调用器的权重值是否相等
        boolean sameWeight = true;

        // 筛选出所有最少活动数的调用器
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // 获取调用器的活动调用数
            int active = RpcStatus.getStatus(invoker.getUrl(), RpcUtils.getMethodName(invocation)).getActive();
            // 获取调用器的权重值，默认为100
            int afterWarmup = getWeight(invoker, invocation);
            // 保存权重值以备后用
            weights[i] = afterWarmup;
            // 如果当前调用器是第一个调用器，或者当前调用器的活动调用数小于最少活动数
            if (leastActive == -1 || active < leastActive) {
                // 将当前调用器的活动调用数设置为最少活动数
                leastActive = active;
                // 将最少活动数个数重置为1
                leastCount = 1;
                // 将第一个最少活动数的调用器索引设置为i
                leastIndexes[0] = i;
                // 重置权重之和
                totalWeight = afterWarmup;
                // 记录第一个最少活动数的调用器的权重值
                firstWeight = afterWarmup;
                // 每个调用器的权重是否相等（只有一个调用器时相等）
                sameWeight = true;
            } else if (active == leastActive) {
                // 将最少活动数的调用器的索引添加到leastIndexes中
                leastIndexes[leastCount++] = i;
                // 累加最少活动数的调用器的权重之和
                totalWeight += afterWarmup;
                // 如果每个调用器的权重都相等，且当前调用器的权重值不等于第一个最少活动数的调用器的权重值，则权重不相等
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // 从所有最少活动数的调用器中选择一个调用器
        if (leastCount == 1) {
            // 如果只有一个最少活动数的调用器，直接选择该调用器
            return invokers.get(leastIndexes[0]);
        }
        if (!sameWeight && totalWeight > 0) {
            // 如果权重不相等且至少有一个调用器的权重大于0，根据权重值随机选择调用器
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // 根据权重值选择一个调用器
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // 如果所有调用器的权重值相等或者总权重值等于0，均匀随机选择调用器
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
