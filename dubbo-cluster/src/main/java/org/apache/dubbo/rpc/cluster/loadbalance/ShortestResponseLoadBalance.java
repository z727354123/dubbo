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
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ShortestResponseLoadBalance
 * </p>
 * Filter the number of invokers with the shortest response time of
 * success calls and count the weights and quantities of these invokers in last slide window.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 *
 * 最小响应时间负载均衡 负载均衡
 * (最小活跃 响应时间)
 *
 * 每个服务器, 响应时间 = 成功请求时间 / 成功数量
 * 再计算得出  最小活跃响应时间  =  响应时间 * (活跃数 + 1)
 * 多个就 权重随机
 * 跟最小活跃度相似, 但是用 最小活跃响应时间 判断
 *
 * 最小活跃响应时间 默认 每 30s 重置一次
 *
 */
public class ShortestResponseLoadBalance extends AbstractLoadBalance implements ScopeModelAware {

    public static final String NAME = "shortestresponse";

    // 重置周期 30s
    private int slidePeriod = 30_000;

    // 对应 每个 服务 状态集合
    private ConcurrentMap<RpcStatus, SlideWindowData> methodMap = new ConcurrentHashMap<>();

    // 判断是否 正在重置的 锁
    private AtomicBoolean onResetSlideWindow = new AtomicBoolean(false);

    // 用于判断重置周期
    private volatile long lastUpdateTime = System.currentTimeMillis();

    // 重置用 线程池
    private ExecutorService executorService;

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        // 初始化 重置周期 线程池
        slidePeriod = applicationModel.getModelEnvironment().getConfiguration().getInt(Constants.SHORTEST_RESPONSE_SLIDE_PERIOD, 30_000);
        executorService = applicationModel.getApplicationExecutorRepository().getSharedExecutor();
    }

    protected static class SlideWindowData {
        // 重置数量
        // A服务的成功数量 是一致累加,
        // 某些情况,需要重置他的 最小响应时间, 重新计算

        // 用于计算的成功数  = 总成功请求数 - 重置数量
        private long succeededOffset;
        // 重置 时间, 重置计算
        // 用于计算的响应时间 = 总成功请求响应时间 - 重置时间
        private long succeededElapsedOffset;
        private RpcStatus rpcStatus;

        public SlideWindowData(RpcStatus rpcStatus) {
            this.rpcStatus = rpcStatus;
            this.succeededOffset = 0;
            this.succeededElapsedOffset = 0;
        }

        public void reset() {
            // 重置, 记录相关信息
            this.succeededOffset = rpcStatus.getSucceeded();
            this.succeededElapsedOffset = rpcStatus.getSucceededElapsed();
        }

        // 获取最小相应时间
        private long getSucceededAverageElapsed() {
            // 成功请求数
            long succeed = this.rpcStatus.getSucceeded() - this.succeededOffset;
            if (succeed == 0) {
                return 0;
            }
            // 成功请求时间 / 成功数
            return (this.rpcStatus.getSucceededElapsed() - this.succeededElapsedOffset) / succeed;
        }

        public long getEstimateResponse() {
            // 当前活跃连接 + 1
            int active = this.rpcStatus.getActive() + 1;
            // 与最小相应时间 相乘
            return getSucceededAverageElapsed() * active;
        }
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int length = invokers.size();
        // 最小 响应时间
        long shortestResponse = Long.MAX_VALUE;
        // 最小响应时间 数
        int shortestCount = 0;
        // 1. 下标数组, 用于获取invokers 下标
        int[] shortestIndexes = new int[length];
        // 权重数组
        int[] weights = new int[length];
        int totalWeight = 0;
        // 最小相应时间 第一个权重, 用于判断 sameWeight
        int firstWeight = 0;
        // 是否 等权
        boolean sameWeight = true;

        // 2. 计算所有 的数值
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
            // 空就初始化
            SlideWindowData slideWindowData = methodMap.computeIfAbsent(rpcStatus, SlideWindowData::new);

            // 最小活跃响应时间
            // 每个服务器, 响应时间 = 成功请求时间 / 成功数量
            // 再计算得出  最小活跃响应时间  =  响应时间 * (活跃数 + 1)
            long estimateResponse = slideWindowData.getEstimateResponse();
            int afterWarmup = getWeight(invoker, invocation);
            weights[i] = afterWarmup;
            // Same as LeastActiveLoadBalance
            if (estimateResponse < shortestResponse) {
                shortestResponse = estimateResponse;
                shortestCount = 1;
                shortestIndexes[0] = i;
                // 维护
                totalWeight = afterWarmup;
                firstWeight = afterWarmup;
                sameWeight = true;
            } else if (estimateResponse == shortestResponse) {
                shortestIndexes[shortestCount++] = i;
                totalWeight += afterWarmup;
                if (sameWeight && i > 0
                    && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }

        // 5. 判断是否过了 30s, 过了就异步 重置
        if (System.currentTimeMillis() - lastUpdateTime > slidePeriod
            && onResetSlideWindow.compareAndSet(false, true)) {
            // 异步重置
            executorService.execute(() -> {
                methodMap.values().forEach(SlideWindowData::reset);
                lastUpdateTime = System.currentTimeMillis();
                onResetSlideWindow.set(false);
            });
        }
        // 3. 单个直接返回
        if (shortestCount == 1) {
            return invokers.get(shortestIndexes[0]);
        }
        // 4. 多个权重随机
        if (!sameWeight && totalWeight > 0) {
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < shortestCount; i++) {
                int shortestIndex = shortestIndexes[i];
                offsetWeight -= weights[shortestIndex];
                if (offsetWeight < 0) {
                    return invokers.get(shortestIndex);
                }
            }
        }
        return invokers.get(shortestIndexes[ThreadLocalRandom.current().nextInt(shortestCount)]);
    }
}
