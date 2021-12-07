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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance.
 * Ngnix 平滑负载均衡轮询
 *
 * 请求编号	currentWeight数组	选择结果	currentWeight数组
 * 1        [5, 2, 1]	        A	    [-3, 2, 1]
 * 2        [2, 4, 2]	        B	    [2, -4, 2]
 * 3        [7, -2, 3]	        A	    [-1, -2, 3]
 * 4        [4, 0, 4]	        A	    [-4, 0, 4]
 * 5        [1, 2, 5]	        C	    [1, 2, -3]
 * 6        [6, 4, -2]	        A	    [-2, 4, -2]
 * 7        [3, 6, -1]	        B	    [3, -2, -1]
 * 8        [8, 0, 0]	        A	    [0, 0, 0]
 *
 *
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";

    private static final int RECYCLE_PERIOD = 60000;

    // 类似于 [5, 2, 1] 数组的单个元素, 封装了 +- 等
    protected static class WeightedRoundRobin {
        // 服务提供者权重
        private int weight;
        // 当前权重
        private AtomicLong current = new AtomicLong(0);
        // 最后一次更新时间, 用于 服务数量不一致 移除 60秒未更新节点
        private long lastUpdate;

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }

        public long increaseCurrent() {
            return current.addAndGet(weight);
        }

        public void sel(int total) {
            current.addAndGet(-1 * total);
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }



    // 嵌套 Map 结构，存储的数据结构示例如下：
    // {
    //     "UserService.query": {
    //         "url1": WeightedRoundRobin@123,
    //         "url2": WeightedRoundRobin@456,
    //     },
    //     "UserService.update": {
    //         "url1": WeightedRoundRobin@123,
    //         "url2": WeightedRoundRobin@456,
    //     }
    // }
    // 最外层为 服务类名 + 方法名，第二层为 url 到 WeightedRoundRobin 的映射关系。
    // 这里我们可以将 url 看成是服务提供者的 id
    // 类似于 [5, 2, 1] 数组, 不过用 ConcurrentMap 代替
    private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<String, ConcurrentMap<String, WeightedRoundRobin>>();

    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     *
     * @param invokers
     * @param invocation
     * @return
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 1. 服务标识: 如 "UserService.query"
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        // 2. 获取 url 到 WeightedRoundRobin 映射表，如果为空，则创建一个新的
        ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        // 总权重 用于选中的 服务 - total
        int totalWeight = 0;
        // 最大权重
        long maxCurrent = Long.MIN_VALUE;
        // 获取当前时间, 维护 lastUpdate
        long now = System.currentTimeMillis();
        Invoker<T> selectedInvoker = null;
        WeightedRoundRobin selectedWRR = null;

        // 下面这个循环主要做了这样几件事情：
        //   1. 遍历 Invoker 列表，检测当前 Invoker 是否有
        //      相应的 WeightedRoundRobin，没有则创建
        //   2. 检测 Invoker 权重是否发生了变化，若变化了，
        //      则更新 WeightedRoundRobin 的 weight 字段
        //   3. 让 current 字段加上自身权重，等价于 current += weight
        //   4. 设置 lastUpdate 字段，即 lastUpdate = now
        //   5. 寻找具有最大 current 的 Invoker，以及 Invoker 对应的 WeightedRoundRobin，
        //      暂存起来，留作后用
        //   6. 计算权重总和
        for (Invoker<T> invoker : invokers) {
            String identifyString = invoker.getUrl().toIdentityString();
            int weight = getWeight(invoker, invocation);
            // 检测当前 Invoker 是否有对应的 WeightedRoundRobin，没有则创建,
            // 类似于 [5, 2, 1] 的单个元素
            // 3. 维护 map {
            //        "url1": WeightedRoundRobin@123,
            //        "url2": WeightedRoundRobin@456,
            //     },
            WeightedRoundRobin weightedRoundRobin = map.computeIfAbsent(identifyString, k -> {
                WeightedRoundRobin wrr = new WeightedRoundRobin();
                wrr.setWeight(weight);
                return wrr;
            });
            // 权重重置 Invoker 权重不等于 WeightedRoundRobin 中保存的权重，说明权重变化了，此时进行更新
            if (weight != weightedRoundRobin.getWeight()) {
                //weight changed
                weightedRoundRobin.setWeight(weight);
            }

            // 4. 让 current 加上自身权重，等价于 current += weight
            long cur = weightedRoundRobin.increaseCurrent();
            weightedRoundRobin.setLastUpdate(now);

            // 5. 找出最大的 current
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedInvoker = invoker;
                selectedWRR = weightedRoundRobin;
            }
            totalWeight += weight;
        }

        // 对 <identifyString, WeightedRoundRobin> 进行检查，过滤掉长时间未被更新的节点。
        // 该节点可能挂了，invokers 中不包含该节点，所以该节点的 lastUpdate 长时间无法被更新。
        // 若未更新时长超过阈值后，就会被移除掉，默认阈值为60秒
        if (invokers.size() != map.size()) {
            map.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD);
        }
        // 6. 返回最大权重
        if (selectedInvoker != null) {
            // 当前选中 - 全部权重和
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }
        // should not happen here
        return invokers.get(0);
    }

}
