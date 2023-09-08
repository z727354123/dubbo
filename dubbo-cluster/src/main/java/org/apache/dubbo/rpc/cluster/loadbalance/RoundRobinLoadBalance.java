/**
 * Round robin load balance.
 * 循环负载均衡算法的实现
 * 该算法会依次轮询选择可用的服务提供者
 */

package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin"; // 负载均衡名称

    private static final int RECYCLE_PERIOD = 60000; // 回收周期

    private final ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<>(); // 方法级别的权重映射表

    protected static class WeightedRoundRobin {
        private int weight;
        private final AtomicLong current = new AtomicLong(0); // 当前权重
        private long lastUpdate; // 最后更新时间

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0); // 设置权重后，重置当前权重为0
        }

        public long increaseCurrent() {
            return current.addAndGet(weight); // 增加当前权重，并返回增加后的值
        }

        public void sel(int total) {
            current.addAndGet(-1 * total); // 减少当前权重
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

    /**
     * 获取缓存的调用地址列表
     * 仅用于单元测试
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + RpcUtils.getMethodName(invocation);
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + RpcUtils.getMethodName(invocation);
        ConcurrentMap<String, WeightedRoundRobin> map = ConcurrentHashMapUtils.computeIfAbsent(methodWeightMap, key, k -> new ConcurrentHashMap<>());
        int totalWeight = 0; // 总权重
        long maxCurrent = Long.MIN_VALUE; // 最大当前权重
        long now = System.currentTimeMillis();
        Invoker<T> selectedInvoker = null; // 被选中的调用者
        WeightedRoundRobin selectedWRR = null; // 被选中的调用者对应的权重对象
        for (Invoker<T> invoker : invokers) {
            String identifyString = invoker.getUrl().toIdentityString(); // 调用者的标识字符串
            int weight = getWeight(invoker, invocation); // 调用者的权重
            WeightedRoundRobin weightedRoundRobin = ConcurrentHashMapUtils.computeIfAbsent(map, identifyString, k -> {
                WeightedRoundRobin wrr = new WeightedRoundRobin();
                wrr.setWeight(weight);
                return wrr;
            });

            if (weight != weightedRoundRobin.getWeight()) {
                // 权重发生变化，更新权重
                weightedRoundRobin.setWeight(weight);
            }
            long cur = weightedRoundRobin.increaseCurrent(); // 增加当前权重，并返回增加后的值
            weightedRoundRobin.setLastUpdate(now); // 更新最后更新时间
            if (cur > maxCurrent) {
                // 选择具有最大当前权重的调用者
                maxCurrent = cur;
                selectedInvoker = invoker;
                selectedWRR = weightedRoundRobin;
            }
            totalWeight += weight;
        }

        // 移除长时间没有更新的权重对象
        if (invokers.size() != map.size()) {
            map.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD);
        }

        if (selectedInvoker != null) {
            selectedWRR.sel(totalWeight); // 减少总权重
            return selectedInvoker;
        }
        // 如果没有找到合适的调用者，则默认选择列表中的第一个
        return invokers.get(0);
    }
}
