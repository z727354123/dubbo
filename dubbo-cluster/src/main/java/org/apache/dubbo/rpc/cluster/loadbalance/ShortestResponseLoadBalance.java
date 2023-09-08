/*
 * ShortestResponseLoadBalance短响应时间负载均衡器
 * </p>
 * 过滤具有最短响应时间的调用器数量，并在最后的滑动时间窗口中计算这些调用器的权重和数量。
 * 如果只有一个调用者，则直接使用该调用者；
 * 如果有多个调用者且权重不相同，则根据总权重随机选择；
 * 如果有多个调用者且权重相同，则随机选择调用。
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShortestResponseLoadBalance extends AbstractLoadBalance implements ScopeModelAware {

    public static final String NAME = "shortestresponse";

    private int slidePeriod = 30_000; // 滑动时间窗口的周期，默认为30秒

    private final ConcurrentMap<RpcStatus, SlideWindowData> methodMap = new ConcurrentHashMap<>(); // 存储每个调用方法对应的滑动窗口数据

    private final AtomicBoolean onResetSlideWindow = new AtomicBoolean(false); // 是否正在重置滑动窗口的标志

    private volatile long lastUpdateTime = System.currentTimeMillis(); // 上次滑动窗口数据更新的时间

    private ExecutorService executorService; // 执行异步任务的线程池

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        slidePeriod = applicationModel.modelEnvironment().getConfiguration().getInt(Constants.SHORTEST_RESPONSE_SLIDE_PERIOD, 30_000); // 从配置中获取滑动时间窗口的周期
        executorService = applicationModel.getFrameworkModel().getBeanFactory()
            .getBean(FrameworkExecutorRepository.class).getSharedExecutor(); // 从应用程序模型中获取共享的执行器
    }

    protected static class SlideWindowData {

        private long succeededOffset; // 滑动窗口的起始成功调用次数
        private long succeededElapsedOffset; // 滑动窗口的起始成功调用总耗时
        private final RpcStatus rpcStatus; // 对应的RpcStatus对象

        public SlideWindowData(RpcStatus rpcStatus) {
            this.rpcStatus = rpcStatus;
            this.succeededOffset = 0;
            this.succeededElapsedOffset = 0;
        }

        public void reset() {
            this.succeededOffset = rpcStatus.getSucceeded();
            this.succeededElapsedOffset = rpcStatus.getSucceededElapsed();
        }

        private long getSucceededAverageElapsed() {
            long succeed = this.rpcStatus.getSucceeded() - this.succeededOffset;
            if (succeed == 0) {
                return 0;
            }
            return (this.rpcStatus.getSucceededElapsed() - this.succeededElapsedOffset) / succeed;
        }

        public long getEstimateResponse() {
            int active = this.rpcStatus.getActive() + 1;
            return getSucceededAverageElapsed() * active; // 估计的调用响应时间，通过活跃连接数与成功调用的平均耗时相乘得到
        }
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 调用者的数量
        int length = invokers.size();
        // 所有调用者中估计的最短响应时间
        long shortestResponse = Long.MAX_VALUE;
        // 估计的最短响应时间相同的调用者数量
        int shortestCount = 0;
        // 估计的最短响应时间相同的调用者的索引
        int[] shortestIndexes = new int[length];
        // 每个调用者的权重值
        int[] weights = new int[length];
        // 所有估计的最短响应时间调用者的加权和
        int totalWeight = 0;
        // 第一个估计的最短响应时间调用者的权重值
        int firstWeight = 0;
        // 所有估计的最短响应时间调用者的权重值是否相等
        boolean sameWeight = true;

        // 过滤出所有估计的最短响应时间调用者
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), RpcUtils.getMethodName(invocation));
            SlideWindowData slideWindowData = ConcurrentHashMapUtils.computeIfAbsent(methodMap, rpcStatus, SlideWindowData::new);

            // 计算估计的响应时间，根据活跃连接数和成功调用的平均耗时的乘积。
            long estimateResponse = slideWindowData.getEstimateResponse();
            int afterWarmup = getWeight(invoker, invocation);
            weights[i] = afterWarmup;
            // 和LeastActiveLoadBalance的实现相同
            if (estimateResponse < shortestResponse) {
                shortestResponse = estimateResponse;
                shortestCount = 1;
                shortestIndexes[0] = i;
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

        if (System.currentTimeMillis() - lastUpdateTime > slidePeriod
            && onResetSlideWindow.compareAndSet(false, true)) {
            //异步方式重置滑动窗口
            executorService.execute(() -> {
                methodMap.values().forEach(SlideWindowData::reset);
                lastUpdateTime = System.currentTimeMillis();
                onResetSlideWindow.set(false);
            });
        }

        if (shortestCount == 1) {
            return invokers.get(shortestIndexes[0]);
        }
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
