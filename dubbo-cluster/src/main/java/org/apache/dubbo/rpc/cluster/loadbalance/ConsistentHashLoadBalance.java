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
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;

/**
 * ConsistentHashLoadBalance
 * 取模问题
 * 取模后/ hash值 / 服务器
 *  0      9      A
 *  1      10     B
 *  2      11     C
 *
 *  0      10     A
 *  1      9  11  B
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "consistenthash";
    /**
     * Hash nodes name
     */
    public static final String HASH_NODES = "hash.nodes";
    /**
     * Hash arguments name
     */
    public static final String HASH_ARGUMENTS = "hash.arguments";

    // 嵌套 Map 结构，存储的数据结构示例如下：
    // {
    //     "UserService.query": ConsistentHashSelector@123,
    //     "UserService.update":  ConsistentHashSelector@456
    // }
    // key为 服务类名 + 方法名, val为 ConsistentHashSelector。
    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();
    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String methodName = RpcUtils.getMethodName(invocation);
        // Service.method   服务.方法, DemoService.method1
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;
        // 1. 获取 invokers 原始的 hashcode, 用于 判断服务列表是否发生编码
        int invokersHashCode = getCorrespondingHashCode(invokers);
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        if (selector == null || selector.identityHashCode != invokersHashCode) {
            // 2. 初始化 ConsistentHashSelector
            selectors.put(key, new ConsistentHashSelector<T>(invokers, methodName, invokersHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        // 3. 进行一致性hash 算法计算
        return selector.select(invocation);
    }

    /**
     * get hash code of invokers
     * Make this method to public in order to use this method in test case
     * @param invokers
     * @return
     */
    public <T> int getCorrespondingHashCode(List<Invoker<T>> invokers){
        return invokers.hashCode();
    }

    private static final class ConsistentHashSelector<T> {

        // 使用 TreeMap 存储 Invoker 虚拟节点
        private final TreeMap<Long, Invoker<T>> virtualInvokers;

        // 虚拟节点数
        private final int replicaNumber;
        // hashcode 用于判断 invokers 变化
        private final int identityHashCode;

        // 下标数组, 对 URL请求参数的 几个下标进行hash  如, [0, 2], 默认 [0]
        private final int[] argumentIndex;

        /**
         * key: server(invoker) address
         * value: count of requests accept by certain server
         * 各个服务 请求数
         * {
         *     "127.0.0.1:1": 数量,
         *     "127.0.0.1:2": 数量,
         *     "127.0.0.1:3": 数量,
         * }
         */
        private Map<String, AtomicLong> serverRequestCountMap = new ConcurrentHashMap<>();

        /**
         * count of total requests accept by all servers
         * 总请求数
         */
        private AtomicLong totalRequestCount;

        /**
         * count of current servers(invokers)
         * 服务 数量
         */
        private int serverCount;

        /**
         * the ratio which allow count of requests accept by each server
         * overrate average (totalRequestCount/serverCount).
         * 1.5 is recommended, in the future we can make this param configurable
         *
         * 服务过载 调整比例 默认 1.5
         * 每个服务器接受请求计数 的 比率
         * 单个服务器 总请求 > (总请求数 / 服务数量) * 1.5 这个比例时, 就会进行相关优化 取下个不过载节点
         *
         * 比如 总共 30 个请求, 有服务 [A, B, C] 接收请求数 [ 15, 10, 5]
         * 第31个请求过来时, ABC, 已经接收的
         * 服务过载数量 (总请求数 / 服务数量) * 1.5  = 15
         * 第31个请求 hash 算得是 服务A, 就会进行相关优化 取下个不过载节点
         *
         */
        private static final double OVERLOAD_RATIO_THREAD = 1.5F;

        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();
            // 1. 虚拟节点数, 默认 160
            this.replicaNumber = url.getMethodParameter(methodName, HASH_NODES, 160);
            // 获取参与 hash 计算的参数下标值，默认对第一个参数进行 hash 运算
            String[] index = COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, HASH_ARGUMENTS, "0"));
            // 2. 下标数组, 对 URL请求参数的 几个下标进行hash  如, [0, 2], 默认 [0]
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            // 3. 计算节点存到 TreeMap
            for (Invoker<T> invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    // 对 address + i 进行 md5 运算，得到一个长度为16的字节数组
                    // address 0 ~ address 39
                    // "127.0.0.1:1" + "0"  ~ "127.0.0.1:1" + "39"
                    // digest 有 16 段
                    // int = 4 byte
                    // a89e84e7bbcfbc1120536123d67982d8
                    // a89e84e7 bbcfbc11 20536123 d67982d8
                    byte[] digest = Bytes.getMD5(address + i);
                    for (int h = 0; h < 4; h++) {
                        // h = 0 时，取 digest 中下标为 0 ~ 3 的4个字节进行位运算
                        // h = 1 时，取 digest 中下标为 4 ~ 7 的4个字节进行位运算
                        // h = 2 时，8 ~ 11
                        // h = 3 时，12 ~ 15
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
            // 维护服务过载的参数
            totalRequestCount = new AtomicLong(0);
            serverCount = invokers.size();
            serverRequestCountMap.clear();
        }

        public Invoker<T> select(Invocation invocation) {
            // 1. 需要 hash 的参数
            String key = toKey(invocation.getArguments());
            byte[] digest = Bytes.getMD5(key);
            // 2. 命中圆环节点
            return selectForKey(hash(digest, 0));
        }
        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            // 需要 hash 的参数 下标数组
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }
        private Invoker<T> selectForKey(long hash) {
            // tree 向下 取 元素,
            // 到 TreeMap 中查找第一个节点值大于或等于当前 hash 的 Entry
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.ceilingEntry(hash);
            // 空则取 TreeMap 最小 Entry
            if (entry == null) {
                entry = virtualInvokers.firstEntry();
            }

            // 后续的 服务过载优化
            String serverAddress = entry.getValue().getUrl().getAddress();

            /**
             * The following part of codes aims to select suitable invoker.
             * This part is not complete thread safety.
             * However, in the scene of consumer-side load balance,
             * thread race for this part of codes
             * (execution time cost for this part of codes without any IO or
             * network operation is very low) will rarely occur. And even in
             * extreme case, a few requests are assigned to an invoker which
             * is above OVERLOAD_RATIO_THREAD will not make a significant impact
             * on the effect of this new algorithm.
             * And make this part of codes synchronized will reduce efficiency of
             * every request. In my opinion, this is not worth. So it is not a
             * problem for this part is not complete thread safety.
             *
             * 以下部分代码旨在选择合适的调用者。
             * 此零件不是完全的线程安全。
             * 但是，在消费者侧负载均衡场景中，
             * 这一部分代码的线程竞争（没有任何IO或网络操作的这一部分代码的执行时间成本非常低）很少发生。
             * 即使在极端情况下，将一些请求分配给高于OVERLOAD_RATIO_THREAD的调用者，也不会对这种新算法的效果产生重大影响。
             * 而让这一部分代码 使用同步会 降低每个请求的效率。在我看来，这不值得。
             * 因此，这不是一个问题，因为这段代码 不是完全的线程安全。
             */
            // 服务过载数量
            double overloadThread = ((double) totalRequestCount.get() / (double) serverCount) * OVERLOAD_RATIO_THREAD;
            /**
             * Find a valid server node:
             * 1. Not have accept request yet
             * or
             * 2. Not have overloaded (request count already accept < thread (average request count * overloadRatioAllowed ))
             */
            // 循环至 没有过载的 服务
            while (serverRequestCountMap.containsKey(serverAddress)
                && serverRequestCountMap.get(serverAddress).get() >= overloadThread) {
                // 获取下一个节点
                entry = getNextInvokerNode(virtualInvokers, entry);
                serverAddress = entry.getValue().getUrl().getAddress();
            }
            // 初始化 1, 或累加, 维护 serverRequestCountMap
            if (!serverRequestCountMap.containsKey(serverAddress)) {
                serverRequestCountMap.put(serverAddress, new AtomicLong(1));
            } else {
                serverRequestCountMap.get(serverAddress).incrementAndGet();
            }
            // 总请求数 + 1
            totalRequestCount.incrementAndGet();

            return entry.getValue();
        }

        private Map.Entry<Long, Invoker<T>> getNextInvokerNode(TreeMap<Long, Invoker<T>> virtualInvokers, Map.Entry<Long, Invoker<T>> entry){
            // higherEntry 获取下一个 更大的节点
            Map.Entry<Long, Invoker<T>> nextEntry = virtualInvokers.higherEntry(entry.getKey());
            if(nextEntry == null){
                return virtualInvokers.firstEntry();
            }
            return nextEntry;
        }

        // 一个 byte 8 位
        // 4个byte 32位
        // 例如:   number = 0 时，取 digest 中下标为 0 ~ 3 的4个字节进行位运算
        // long = 8byte
        // 4个byte 空,   [byte3][byte2][byte1][byte0]
        // 3 2 1 0
        // 例如:   number = 1 时，取 digest 中下标为 4 ~ 7 的4个字节进行位运算
        // 4个byte 空,   [byte3][byte2][byte1][byte0]
        // 7 6 5 4
        // 故返回最大值 也就 0xFFFFFFFFL = 2^32 -1
        // 使用 Long 返回 保证 正数
        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }

        public static void main(String[] args) {
            System.out.println(0xFFFFFFFFL);
            System.out.println((1L<<32) -1);
            System.out.println(Integer.MAX_VALUE);
            System.out.println((int) 0xFFFFFFFFL);

        }
    }

}
