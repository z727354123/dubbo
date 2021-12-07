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
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

/**
 * This class select one provider from multiple providers randomly.
 * You can define weights for each provider:
 * If the weights are all the same then it will use random.nextInt(number of invokers).
 * If the weights are different then it will use random.nextInt(w1 + w2 + ... + wn)
 * Note that if the performance of the machine is better than others, you can set a larger weight.
 * If the performance is not so good, you can set a smaller weight.
 * 加权随机 策略, 加权随机算法的具体实现
 * 假设 服务器 servers = [A, B, C]
 *       权重 weights = [5, 3, 2]
 *   生成随机数字 [0,10)  A = [0, 5)  B = [5, 8)  C = [8, 10)
 *               0-9          0-4         5-7         8-9
 *
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    /**
     * Select one invoker between a list using a random criteria
     * @param invokers List of possible invokers
     * @param url URL
     * @param invocation Invocation
     * @param <T>
     * @return The selected invoker
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int length = invokers.size();
        // 1. 总体过滤, url 不带 weigth/timestamp 等权重 随机
        // 无 weight 配置, 且无 启动时间, 直接 等权重 随机
        if (!needWeightLoadBalance(invokers,invocation)){
            // 2. 使用 ThreadLocalRandom 给每个线程创建自己的 SEED,
            /** 改善 多线程 使用单个 {@link Random#nextInt} 并发问题 */

            return invokers.get(ThreadLocalRandom.current().nextInt(length));
        }

        // 每个 服务提供者 权重相等
        boolean sameWeight = true;
        // the maxWeight of every invokers, the minWeight = 0 or the maxWeight of the last invoker
        // 3. 权重 区间数组
        /**
         * 假设 服务器  = [A, B, C]
         *       权重  = [5, 3, 2]
         * 数组 weights= [5, 8, 10]
         */
        int[] weights = new int[length];
        // 总权重
        int totalWeight = 0;
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            // 累加
            totalWeight += weight;
            weights[i] = totalWeight;
            // 当前权重 * 遍历数量 与 总权重 比较,  来判断每个权重是否相等
            if (sameWeight && totalWeight != weight * (i + 1)) {
                sameWeight = false;
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // 4. 生成 随机数 0~9 判断权重
            int offset = ThreadLocalRandom.current().nextInt(totalWeight); // 10
            for (int i = 0; i < length; i++) {
                if (offset < weights[i]) {
                    return invokers.get(i);
                }
            }
        }
        // 每个服务权重一样
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }

    /**
     * 判断 是否需要 加权重 随机
     */
    private <T> boolean needWeightLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {

        Invoker invoker = invokers.get(0);
        URL invokerUrl = invoker.getUrl();
        // RegistryService, 访问注册中心, 不关注 服务运行时长, 只关注配置
        if (REGISTRY_SERVICE_REFERENCE_PATH.equals(invokerUrl.getServiceInterface())) {
            // 有此配置 registry.weight 就需要
            String weight = invokerUrl.getParameter(REGISTRY_KEY + "." + WEIGHT_KEY);
            if (StringUtils.isNotEmpty(weight)) {
                return true;
            }
        } else {
            String weight = invokerUrl.getMethodParameter(invocation.getMethodName(), WEIGHT_KEY);
            if (StringUtils.isNotEmpty(weight)) {
                // 有 配置的权重 weight
                return true;
            }else {
                // 或 服务启动时间
                String timeStamp = invoker.getUrl().getParameter(TIMESTAMP_KEY);
                if (StringUtils.isNotEmpty(timeStamp)) {
                    return true;
                }
            }
        }
        return false;
    }


}
