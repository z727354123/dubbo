/*
 * 这个类是Dubbo负载均衡中的一个具体实现，提供了随机选择一个服务提供者的功能。
 * 可以为每个服务提供者定义一个权重，如果权重都相同，则使用随机数选择一个服务提供者；
 * 如果权重不同，则使用根据权重进行随机选择。
 * 当某个机器的性能较好时，可以设置较大的权重；当性能较差时，可以设置较小的权重。
 */

package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

/**
 * 从多个服务提供者中随机选择一个进行调用的负载均衡算法实现。
 * 你可以为每个服务提供者定义权重：
 * 如果所有服务提供者的权重相同，将使用random.nextInt(number of invokers)选择；
 * 如果权重不同，将使用random.nextInt(w1 + w2 + ... + wn)选择；
 * 注意，如果某个机器的性能比其他机器好，可以设置较大的权重；
 * 如果性能不好，可以设置较小的权重。
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    /**
     * 根据随机算法从服务提供者列表中选择一个服务提供者进行调用
     *
     * @param invokers   可能的服务提供者列表
     * @param url        URL
     * @param invocation 调用信息
     * @param <T>
     * @return 被选择的服务提供者
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 服务提供者个数
        int length = invokers.size();

        if (!needWeightLoadBalance(invokers, invocation)) {
            return invokers.get(ThreadLocalRandom.current().nextInt(length));
        }

        // 每个服务提供者的权重是否相同
        boolean sameWeight = true;
        // 每个服务提供者的最大权重，最小权重为0或最后一个服务提供者的权重
        int[] weights = new int[length];
        // 总权重
        int totalWeight = 0;
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            // 累加权重
            totalWeight += weight;
            // 保存权重供后面使用
            weights[i] = totalWeight;
            if (sameWeight && totalWeight != weight * (i + 1)) {
                sameWeight = false;
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // 如果（不是所有服务提供者的权重相同 & 至少有一个服务提供者的权重>0）,根据总权重选择随机值。
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            // 根据随机值选择一个服务提供者
            if (length <= 4) {
                for (int i = 0; i < length; i++) {
                    if (offset < weights[i]) {
                        return invokers.get(i);
                    }
                }
            } else {
                int i = Arrays.binarySearch(weights, offset);
                if (i < 0) {
                    i = -i - 1;
                } else {
                    while (weights[i+1] == offset) {
                        i++;
                    }
                    i++;
                }
                return invokers.get(i);
            }
        }
        // 如果所有服务提供者的权重相同或者总权重=0，则均匀返回一个服务提供者。
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }

    /**
     * 判断是否需要进行权重负载均衡
     *
     * @param invokers    服务提供者列表
     * @param invocation 调用信息
     * @param <T>
     * @return 是否需要进行权重负载均衡
     */
    private <T> boolean needWeightLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {
        Invoker<T> invoker = invokers.get(0);
        URL invokerUrl = invoker.getUrl();
        if (invoker instanceof ClusterInvoker) {
            invokerUrl = ((ClusterInvoker<?>) invoker).getRegistryUrl();
        }

        // 多注册中心场景，多个注册中心之间进行负载均衡
        if (REGISTRY_SERVICE_REFERENCE_PATH.equals(invokerUrl.getServiceInterface())) {
            String weight = invokerUrl.getParameter(WEIGHT_KEY);
            return StringUtils.isNotEmpty(weight);
        } else {
            String weight = invokerUrl.getMethodParameter(RpcUtils.getMethodName(invocation), WEIGHT_KEY);
            if (StringUtils.isNotEmpty(weight)) {
                return true;
            } else {
                String timeStamp = invoker.getUrl().getParameter(TIMESTAMP_KEY);
                return StringUtils.isNotEmpty(timeStamp);
            }
        }
    }
}
