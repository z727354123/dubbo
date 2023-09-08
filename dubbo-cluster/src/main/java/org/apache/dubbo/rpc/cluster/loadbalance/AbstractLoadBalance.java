/**
 * AbstractLoadBalance 抽象类是一个加载均衡的基类，它实现了 LoadBalance 接口。
 *
 * 它定义了一些抽象方法，并提供了一些公共的方法，以供具体的负载均衡算法类继承和实现。
 */

package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WARMUP;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WEIGHT;
import static org.apache.dubbo.rpc.cluster.Constants.WARMUP_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

public abstract class AbstractLoadBalance implements LoadBalance {
    /**
     * 根据启动时间和预热时间计算权重
     * 新的权重在1到weight之间（包括1和weight）
     *
     * @param uptime 启动时间，单位是毫秒
     * @param warmup 预热时间，单位是毫秒
     * @param weight 调用者的权重
     * @return 考虑预热时间后的权重
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        int ww = (int) ( uptime / ((float) warmup / weight));
        return ww < 1 ? 1 : (Math.min(ww, weight));
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }
        if (invokers.size() == 1) {
            return invokers.get(0);
        }
        return doSelect(invokers, url, invocation);
    }

    /**
     * 选择一个调用者
     *
     * @param invokers 调用者列表
     * @param url URL 对象
     * @param invocation Invocation 对象
     * @return 选中的调用者
     */
    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);


    /**
     * 获得调用者的权重，考虑预热时间
     * 如果调用者运行时间不足预热时间，那么权重会按比例降低
     *
     * @param invoker 调用者
     * @param invocation 该调用者的调用
     * @return 调用者的权重
     */
    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        int weight;
        URL url = invoker.getUrl();
        if (invoker instanceof ClusterInvoker) {
            url = ((ClusterInvoker<?>) invoker).getRegistryUrl();
        }

        // 多个注册中心的情况下，在多个注册中心之间进行负载均衡
        if (REGISTRY_SERVICE_REFERENCE_PATH.equals(url.getServiceInterface())) {
            weight = url.getParameter(WEIGHT_KEY, DEFAULT_WEIGHT);
        } else {
            weight = url.getMethodParameter(RpcUtils.getMethodName(invocation), WEIGHT_KEY, DEFAULT_WEIGHT);
            if (weight > 0) {
                long timestamp = invoker.getUrl().getParameter(TIMESTAMP_KEY, 0L);
                if (timestamp > 0L) {
                    long uptime = System.currentTimeMillis() - timestamp;
                    if (uptime < 0) {
                        return 1;
                    }
                    int warmup = invoker.getUrl().getParameter(WARMUP_KEY, DEFAULT_WARMUP);
                    if (uptime > 0 && uptime < warmup) {
                        weight = calculateWarmupWeight((int)uptime, warmup, weight);
                    }
                }
            }
        }
        return Math.max(weight, 0);
    }
}
