/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.cluster.flow;

import com.alibaba.csp.sentinel.cluster.TokenResultStatus;
import com.alibaba.csp.sentinel.cluster.TokenResult;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.flow.statistic.ClusterMetricStatistics;
import com.alibaba.csp.sentinel.cluster.flow.statistic.limit.GlobalRequestLimiter;
import com.alibaba.csp.sentinel.cluster.server.config.ClusterServerConfigManager;
import com.alibaba.csp.sentinel.cluster.flow.statistic.data.ClusterFlowEvent;
import com.alibaba.csp.sentinel.cluster.flow.statistic.metric.ClusterMetric;
import com.alibaba.csp.sentinel.cluster.server.log.ClusterServerStatLogUtil;
import com.alibaba.csp.sentinel.slots.block.ClusterRuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;

/**
 * Flow checker for cluster flow rules.
 *
 * @author Eric Zhao
 * @since 1.4.0
 */
final class ClusterFlowChecker {

    private static double calcGlobalThreshold(FlowRule rule) {
        double count = rule.getCount();
        switch (rule.getClusterConfig().getThresholdType()) {
            case ClusterRuleConstant.FLOW_THRESHOLD_GLOBAL:
                return count;
            case ClusterRuleConstant.FLOW_THRESHOLD_AVG_LOCAL:
            default:
                int connectedCount = ClusterFlowRuleManager.getConnectedCount(rule.getClusterConfig().getFlowId());
                return count * connectedCount;
        }
    }

    static boolean allowProceed(long flowId) {
        String namespace = ClusterFlowRuleManager.getNamespace(flowId);
        return GlobalRequestLimiter.tryPass(namespace);
    }

    static TokenResult acquireClusterToken(/*@Valid*/ FlowRule rule, int acquireCount, boolean prioritized) {
        Long id = rule.getClusterConfig().getFlowId();

        /*
         * 首先判断是否允许本次许可申请，这是因为 TokenServe 支持嵌入式，即支持在应用节点中嵌入一个 TokenServer，
         * 为了保证许可申请的请求不对正常业务造成比较大的影响，故对申请许可这个动作进行了限流。
         * 一旦触发了限流，将向客户端返回 TOO_MANY_REQUEST 状态码，Sentinel 支持按 namespace 进行限流，
         * 具体由 GlobalRequestLimiter 实现，该类的内部同样基于滑动窗口进行收集，原理与 FlowSlot 相似，，默认的限流TPS为3W，
         */
        if (!allowProceed(id)) {
            //客户端将会降级为单机限流
            return new TokenResult(TokenResultStatus.TOO_MANY_REQUEST);
        }

        //根据流程ID获取指标采集器。
        ClusterMetric metric = ClusterMetricStatistics.getMetric(id);
        if (metric == null) {
            //客户端将会降级为单机限流
            return new TokenResult(TokenResultStatus.FAIL);
        }

        //获取当前正常访问的QPS。
        double latestQps = metric.getAvg(ClusterFlowEvent.PASS);
        /*
         * 根据限流配置规则得出其总许可数量，其主要根据阔值的方式其有所不同，其配置阔值有两种方式：
         * 1. FLOW_THRESHOLD_GLOBAL
         * 总数，即集群中的许可等于限流规则中配置的 count 值。
         * 2. FLOW_THRESHOLD_AVG_LOCAL
         * 单机分摊模式，此时限流规则中配置的值只是单机的 count 值，集群中的许可数等于 count * 集群中客户端的个数。
         * 注意：这里还可以通过 exceedCount 设置来运行超过其最大阔值，默认为1表示不允许超过。
         */
        double globalThreshold = calcGlobalThreshold(rule) * ClusterServerConfigManager.getExceedCount();
        //表示处理完本次请求后剩余的许可数量。
        double nextRemaining = globalThreshold - latestQps - acquireCount;

        //如果剩余的许可数大于0，则本次申请成功，将当前的调用计入指标采集器中，然后返回给客户即可。
        if (nextRemaining >= 0) {
            // TODO: checking logic and metric operation should be separated.
            metric.add(ClusterFlowEvent.PASS, acquireCount);
            metric.add(ClusterFlowEvent.PASS_REQUEST, 1);
            if (prioritized) {
                // Add prioritized pass.
                metric.add(ClusterFlowEvent.OCCUPIED_PASS, acquireCount);
            }
            // Remaining count is cut down to a smaller integer.
            return new TokenResult(TokenResultStatus.OK)
                .setRemaining((int) nextRemaining)
                .setWaitInMs(0);
        } else {
            //当前许可数不足的情况，并且该请求为高优先级的处理逻辑：
            if (prioritized) {
                // Try to occupy incoming buckets.
                ////获取当前等待的TPS（即1s为维度，当前等待的请求数量）
                double occupyAvg = metric.getAvg(ClusterFlowEvent.WAITING);
                if (occupyAvg <= ClusterServerConfigManager.getMaxOccupyRatio() * globalThreshold) {
                    //如果当前等待的TPS低于可借用未来窗口的许可阔值时，可通过，但设置其等待时间，可以通过 maxOccupyRatio 来设置借用的最大比值。
                    int waitInMs = metric.tryOccupyNext(ClusterFlowEvent.PASS, acquireCount, globalThreshold);
                    // waitInMs > 0 indicates pre-occupy incoming buckets successfully.
                    if (waitInMs > 0) {
                        ClusterServerStatLogUtil.log("flow|waiting|" + id);
                        return new TokenResult(TokenResultStatus.SHOULD_WAIT)
                            .setRemaining(0)
                            .setWaitInMs(waitInMs);
                    }
                    // Or else occupy failed, should be blocked.
                }
            }
            // Blocked.
            //如果当前许可不足，并且该请求为普通优先级的处理逻辑，增加阻塞相关指标的统计数，并返回 BLOCKED。
            metric.add(ClusterFlowEvent.BLOCK, acquireCount);
            metric.add(ClusterFlowEvent.BLOCK_REQUEST, 1);
            ClusterServerStatLogUtil.log("flow|block|" + id, acquireCount);
            ClusterServerStatLogUtil.log("flow|block_request|" + id, 1);
            if (prioritized) {
                // Add prioritized block.
                metric.add(ClusterFlowEvent.OCCUPIED_BLOCK, acquireCount);
                ClusterServerStatLogUtil.log("flow|occupied_block|" + id, 1);
            }

            return blockedResult();
        }
    }

    private static TokenResult blockedResult() {
        return new TokenResult(TokenResultStatus.BLOCKED)
            .setRemaining(0)
            .setWaitInMs(0);
    }

    private ClusterFlowChecker() {}
}
