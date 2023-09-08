/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// 导入相关类
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.LoadbalanceRules;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.AdaptiveMetrics;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.LOADBALANCE_KEY;

/**
 * 自适应负载均衡器
 */
public class AdaptiveLoadBalance extends AbstractLoadBalance {

    // 名称常量
    public static final String NAME = "adaptive";

    // 默认key
    private String attachmentKey = "mem,load";

    // 自适应指标
    private final AdaptiveMetrics adaptiveMetrics;

    // 构造函数
    public AdaptiveLoadBalance(ApplicationModel scopeModel) {
        adaptiveMetrics = scopeModel.getBeanFactory().getBean(AdaptiveMetrics.class);
    }

    // 选择调用者
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 选择调用者
        Invoker<T> invoker = selectByP2C(invokers, invocation);

        // 设置附加属性，这里设置了适应性负载均衡的附加属性
        invocation.setAttachment(Constants.ADAPTIVE_LOADBALANCE_ATTACHMENT_KEY, attachmentKey);
        // 获取当前时间作为适应性负载均衡开始时间
        long startTime = System.currentTimeMillis();
        // 将适应性负载均衡开始时间存储到Invocation对象的Attributes中
        invocation.getAttributes().put(Constants.ADAPTIVE_LOADBALANCE_START_TIME, startTime);
        // 设置负载均衡策略为适应性负载均衡
        invocation.getAttributes().put(LOADBALANCE_KEY, LoadbalanceRules.ADAPTIVE);

        // 添加消费者请求数
        // 适应性负载均衡度量类adaptiveMetrics记录了每个消费者的请求数和选择时间
        adaptiveMetrics.addConsumerReq(getServiceKey(invoker, invocation));
        // 在adaptiveMetrics中记录当前消费者的选择时间
        adaptiveMetrics.setPickTime(getServiceKey(invoker, invocation), startTime);

        return invoker;
    }
    // P2C选择调用者
    // 从给定的调用者列表中选择一个调用者
    private <T> Invoker<T> selectByP2C(List<Invoker<T>> invokers, Invocation invocation) {
        // 获取调用者列表的大小
        int length = invokers.size();
        if (length == 1) {
            // 如果只有一个调用者，则直接返回该调用者
            return invokers.get(0);
        }

        if (length == 2) {
            // 如果有两个调用者，则调用chooseLowLoadInvoker方法选择一个负载较低的调用者
            return chooseLowLoadInvoker(invokers.get(0), invokers.get(1), invocation);
        }

        // 随机选择两个不同的位置
        int pos1 = ThreadLocalRandom.current().nextInt(length);
        int pos2 = ThreadLocalRandom.current().nextInt(length - 1);
        if (pos2 >= pos1) {
            pos2 = pos2 + 1;
        }

        // 调用chooseLowLoadInvoker方法选择两个位置上的调用者中负载较低的调用者
        return chooseLowLoadInvoker(invokers.get(pos1), invokers.get(pos2), invocation);
    }

    // 获取服务键
    private String getServiceKey(Invoker<?> invoker, Invocation invocation) {
        String key = (String) invocation.getAttributes().get(invoker);
        if (StringUtils.isNotEmpty(key)) {
            return key;
        }

        key = buildServiceKey(invoker, invocation);
        invocation.getAttributes().put(invoker, key);
        return key;
    }

    // 构建服务键
    private String buildServiceKey(Invoker<?> invoker, Invocation invocation) {
        URL url = invoker.getUrl();
        StringBuilder sb = new StringBuilder(128);
        sb.append(url.getAddress()).append(":").append(invocation.getProtocolServiceKey());
        return sb.toString();
    }

    // 获取超时时间
    private int getTimeout(Invoker<?> invoker, Invocation invocation) {
        URL url = invoker.getUrl();
        String methodName = RpcUtils.getMethodName(invocation);
        return (int) RpcUtils.getTimeout(url, methodName, RpcContext.getClientAttachment(), invocation, DEFAULT_TIMEOUT);
    }
    // 选择负载较低的调用者
    private <T> Invoker<T> chooseLowLoadInvoker(Invoker<T> invoker1, Invoker<T> invoker2, Invocation invocation) {
        int weight1 = getWeight(invoker1, invocation); // 获取调用者1的权重
        int weight2 = getWeight(invoker2, invocation); // 获取调用者2的权重
        int timeout1 = getTimeout(invoker1, invocation); // 获取调用者1的超时时间
        int timeout2 = getTimeout(invoker2, invocation); // 获取调用者2的超时时间
        long load1 = Double.doubleToLongBits(adaptiveMetrics.getLoad(getServiceKey(invoker1, invocation), weight1, timeout1)); // 获取调用者1的负载
        long load2 = Double.doubleToLongBits(adaptiveMetrics.getLoad(getServiceKey(invoker2, invocation), weight2, timeout2)); // 获取调用者2的负载

        if (load1 == load2) {
            // 权重之和
            int totalWeight = weight1 + weight2; // 计算两个调用者的权重之和
            if (totalWeight > 0) {
                int offset = ThreadLocalRandom.current().nextInt(totalWeight); // 生成一个范围在 [0, totalWeight) 的随机数
                if (offset < weight1) {
                    return invoker1; // 如果随机数在调用者1的权重范围内，则选择调用者1
                }
                return invoker2; // 否则选择调用者2
            }
            return ThreadLocalRandom.current().nextInt(2) == 0 ? invoker1 : invoker2; // 如果两个调用者的权重之和为0，则随机选择一个调用者
        }
        return load1 > load2 ? invoker2 : invoker1; // 返回负载较低的调用者
    }

}
