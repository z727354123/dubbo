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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * adaptive Metrics statistics.
 *//*
 * AdaptiveMetrics类用于计算和获取动态指标
 */

public class AdaptiveMetrics {

    private final ConcurrentMap<String, AdaptiveMetrics> metricsStatistics = new ConcurrentHashMap<>();

    private long currentProviderTime = 0;
    private double providerCPULoad = 0;
    private long lastLatency = 0;
    private long currentTime = 0;

    // 允许一定的时间失序
    private long pickTime = System.currentTimeMillis();

    private double beta = 0.5;
    private final AtomicLong consumerReq = new AtomicLong();
    private final AtomicLong consumerSuccess = new AtomicLong();
    private final AtomicLong errorReq = new AtomicLong();
    private double ewma = 0;

    /**
     * 获取负载
     *
     * @param idKey   ID键值
     * @param weight  权重
     * @param timeout 超时时间
     * @return 负载值
     */
    public double getLoad(String idKey, int weight, int timeout) {
        AdaptiveMetrics metrics = getStatus(idKey);

        // 如果时间间隔超过两倍timeout，则强制选择
        if (System.currentTimeMillis() - metrics.pickTime > timeout * 2) {
            return 0;
        }

        // 如果当前时间大于0
        if (metrics.currentTime > 0) {
            // 计算时间倍数
            // 大于超时时间的倍数
            long multiple = (System.currentTimeMillis() - metrics.currentTime) / timeout + 1;
            // 如果倍数大于0
            if (multiple > 0) {
                // 如果当前提供者时间等于当前时间
                if (metrics.currentProviderTime == metrics.currentTime) {
                    // 最后延迟等于超时时间的两倍
                    metrics.lastLatency = timeout * 2L;
                } else {
                    // 最后延迟右移 multiple 位
                    metrics.lastLatency = metrics.lastLatency >> multiple;
                }
                // 更新 ewma 指数加权移动平均数
                metrics.ewma = metrics.beta * metrics.ewma + (1 - metrics.beta) * metrics.lastLatency;
                // 当前时间更新为系统当前时间
                metrics.currentTime = System.currentTimeMillis();
            }
        }

        // 计算当前请求数量
        long inflight = metrics.consumerReq.get() - metrics.consumerSuccess.get() - metrics.errorReq.get();
        // 计算负载值
        return metrics.providerCPULoad * (Math.sqrt(metrics.ewma) + 1) * (inflight + 1) / ((((double) metrics.consumerSuccess.get() / (double) (metrics.consumerReq.get() + 1)) * weight) + 1);
    }

    /**
     * 获取指定idKey的Metrics对象
     *
     * @param idKey 指标键
     * @return Metrics对象
     */
    public AdaptiveMetrics getStatus(String idKey) {
        return ConcurrentHashMapUtils.computeIfAbsent(metricsStatistics, idKey, k -> new AdaptiveMetrics());
    }

    /**
     * 增加消费请求
     *
     * @param idKey 指标键
     */
    public void addConsumerReq(String idKey) {
        AdaptiveMetrics metrics = getStatus(idKey);
        metrics.consumerReq.incrementAndGet();
    }

    /**
     * 增加消费成功数量
     *
     * @param idKey 指标键
     */
    public void addConsumerSuccess(String idKey) {
        AdaptiveMetrics metrics = getStatus(idKey);
        metrics.consumerSuccess.incrementAndGet();
    }

    /**
     * 增加错误请求数量
     *
     * @param idKey 指标键
     */
    public void addErrorReq(String idKey) {
        AdaptiveMetrics metrics = getStatus(idKey);
        metrics.errorReq.incrementAndGet();
    }

    /**
     * 设置选择时间
     *
     * @param idKey 指标键
     * @param time  时间戳
     */
    public void setPickTime(String idKey, long time) {
        AdaptiveMetrics metrics = getStatus(idKey);
        metrics.pickTime = time;
    }

    /**
     * 设置提供者指标
     *
     * @param idKey      指标键
     * @param metricsMap 指标映射
     */
    public void setProviderMetrics(String idKey, Map<String, String> metricsMap) {

        AdaptiveMetrics metrics = getStatus(idKey);

        long serviceTime = Long.parseLong(Optional.ofNullable(metricsMap.get("curTime")).filter(v -> StringUtils.isNumeric(v, false)).orElse("0"));
        // 如果服务器时间小于当前时间，则丢弃
        if (metrics.currentProviderTime > serviceTime) {
            return;
        }

        metrics.currentProviderTime = serviceTime;
        metrics.currentTime = serviceTime;
        metrics.providerCPULoad = Double.parseDouble(Optional.ofNullable(metricsMap.get("load")).filter(v -> StringUtils.isNumeric(v, true)).orElse("0"));
        metrics.lastLatency = Long.parseLong((Optional.ofNullable(metricsMap.get("rt")).filter(v -> StringUtils.isNumeric(v, false)).orElse("0")));

        metrics.beta = 0.5;
        // Dt =  β * Dt-1 + (1 -  β ) * θt (也叫做ewma，指数加权移动平均)
        metrics.ewma = metrics.beta * metrics.ewma + (1 - metrics.beta) * metrics.lastLatency;

    }
}
