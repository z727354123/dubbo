
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

// 声明包名
package org.apache.dubbo.rpc.cluster.loadbalance;

// 导入相关包
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.loadbalance.AbstractLoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;

/**
 * ConsistentHashLoadBalance
 * 一致性哈希负载均衡算法
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {
    // 算法名称
    public static final String NAME = "consistenthash";

    // 哈希节点名称参数
    public static final String HASH_NODES = "hash.nodes";

    // 哈希参数名称参数
    public static final String HASH_ARGUMENTS = "hash.arguments";

    // 保存方法的哈希选择器
    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 获取方法名
        String methodName = RpcUtils.getMethodName(invocation);
        // 构建方法唯一标识
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;
        // 使用invokers的哈希码计算哈希，只关注列表中的元素
        int invokersHashCode = invokers.hashCode();
        // 获取对应的选择器
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        // 如果选择器为null，或者选择器的哈希值不等于当前invokers的哈希值，需要重新创建选择器
        if (selector == null || selector.identityHashCode != invokersHashCode) {
            selectors.put(key, new ConsistentHashSelector<T>(invokers, methodName, invokersHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        // 根据选择器选择相应的invoker
        return selector.select(invocation);
    }
    // 一致性哈希选择器类
    private static final class ConsistentHashSelector<T> {

        // 存储虚拟节点的有序映射表
        private final TreeMap<Long, Invoker<T>> virtualInvokers;

        // 哈希环上的复制节点数
        private final int replicaNumber;

        // 节点在哈希环上的标识码
        private final int identityHashCode;

        // 方法参数的下标
        private final int[] argumentIndex;

        // 构造方法，初始化一致性哈希选择器
        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            // 初始化虚拟节点映射表
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            // 设置节点的标识码
            this.identityHashCode = identityHashCode;
            // 获取方法的参数配置
            URL url = invokers.get(0).getUrl();
            // 获取哈希节点数，默认为160
            this.replicaNumber = url.getMethodParameter(methodName, HASH_NODES, 160);
            // 获取哈希参数下标，默认为0
            String[] index = COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, HASH_ARGUMENTS, "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            // 将节点及其虚拟节点添加到映射表中
            for (Invoker<T> invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    byte[] digest = Bytes.getMD5(address + i);
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        // 根据方法调用选择节点
        public Invoker<T> select(Invocation invocation) {
            byte[] digest = Bytes.getMD5(RpcUtils.getMethodName(invocation));
            return selectForKey(hash(digest, 0));
        }

        // 将参数数组转为字符串作为键
        private String toKey(Object[] args, boolean isGeneric) {
            return isGeneric ? toKey((Object[]) args[1]) : toKey(args);
        }

        // 将参数数组转为字符串作为键
        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && args != null && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        // 根据哈希值选择节点
        private Invoker<T> selectForKey(long hash) {
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.ceilingEntry(hash);
            if (entry == null) {
                entry = virtualInvokers.firstEntry();
            }
            return entry.getValue();
        }

        // 计算哈希值
        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                | (digest[number * 4] & 0xFF))
                & 0xFFFFFFFFL;
        }
    }

}
