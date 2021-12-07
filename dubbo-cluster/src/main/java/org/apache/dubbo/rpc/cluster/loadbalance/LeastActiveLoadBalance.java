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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 *
 * 每个服务提供者对应一个活跃数 active, 由当前客户端 记录
 * 每发起一个请求，活跃数加 1，完成请求后则将活跃数减 1
 * 也就是某一个时刻, 服务正在运行 请求越小, 就调用该服务
 *
 * 假设 服务器 servers = [A, B, C]
 *            actives = [3, 2, 5]
 *            取 B
 * 假设 服务器 servers = [A, B, C]
 *            actives = [5, 2, 2]
 *            weights = [2, 2, 1]
 *            BC 加权随机一个
 *
 * 最小的如果只有一个 , 取最小,
 * 多个, 该范围内 权重随机
 *
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int length = invokers.size();
        /**
         * 1. 假设 服务器 servers = [A, B, C]
         *  *            actives = [5, 2, 2]
         *  *            weights = [3, 2, 1]
         */
        // 最小的活跃数
        int leastActive = -1;
        // 具有相同 “最小活跃数” 数量  2, 也就是 下标数组的有效数量
        int leastCount = 0;
        // 2. 下标数组 记录 多个最小活跃数 下标   [1, 2, 0]
        int[] leastIndexes = new int[length];
        // 权重数组     [3, 2, 1]
        int[] weights = new int[length];
        // 多个最小活跃数  总权重 3
        int totalWeight = 0;
        // 第一个最小活跃数的权重, 用于维护 sameWeight, 2
        int firstWeight = 0;
        // 多个最小活跃数 权重是否相等  false
        boolean sameWeight = true;


        // 2. 遍历服务
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // 获取 活跃度 + 权重
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            int afterWarmup = getWeight(invoker, invocation);
            // 权重数组
            weights[i] = afterWarmup;
            // 3. 初次 或 有最小活跃度, 重置相关
            if (leastActive == -1 || active < leastActive) {
                // 最小活跃度
                leastActive = active;
                // 最小活跃度 数量
                leastCount = 1;
                // 4. 维护 下标数组
                leastIndexes[0] = i;
                // Reset totalWeight
                totalWeight = afterWarmup;
                // 第一个最小活跃度 权重, 用于维护 sameWeight
                firstWeight = afterWarmup;
                // 重置 权重相等
                sameWeight = true;
            } else if (active == leastActive) {
                // 5. 其余相等的最小
                // 维护 下标数组 + 数量
                leastIndexes[leastCount++] = i;
                // 维护 总权重
                totalWeight += afterWarmup;
                // 维护 sameWeight
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // 6. 单个最小活跃直接返回
        if (leastCount == 1) {
            return invokers.get(leastIndexes[0]);
        }
        // 7. 多个权重随机
        if (!sameWeight && totalWeight > 0) {
            // 权重不等随机
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // 权重相等随机一个
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
