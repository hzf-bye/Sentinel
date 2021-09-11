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
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.node.OccupyTimeoutProperty;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.PriorityWaitException;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;
import com.alibaba.csp.sentinel.slots.statistic.StatisticSlot;
import com.alibaba.csp.sentinel.util.TimeUtil;

/**
 * Default throttling controller (immediately reject strategy).
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 */
public class DefaultController implements TrafficShapingController {

    private static final int DEFAULT_AVG_USED_TOKENS = 0;

    /**
     * 基于grade的流控阈值 每秒的请求数或者当前的并发线程数
     */
    private double count;

    /**
     * 流量控制的阈值类型
     * 0-并发线程数，1-QPS
     * @see RuleConstant#FLOW_GRADE_THREAD
     * @see RuleConstant#FLOW_GRADE_QPS
     */
    private int grade;

    public DefaultController(double count, int grade) {
        this.count = count;
        this.grade = grade;
    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        //当前已消耗的令牌数量，即当前时间窗口内已创建的线程数量(FLOW_GRADE_THREAD) 或已通过的请求个数(FLOW_GRADE_QPS)。
        int curCount = avgUsedTokens(node);
        //count为流控规则中配置的阔值(即一个时间窗口中总的令牌个数)x
        if (curCount + acquireCount > count) {
            //如果当前时间窗口剩余令牌数小于需要申请的令牌数，则需要根据是否有优先级进行不同的处理。
            if (prioritized && grade == RuleConstant.FLOW_GRADE_QPS) {
                //如果该请求存在优先级，即 prioritized 设置为 true，并且流控类型为基于QPS进行限流，则进入相关的处理逻辑
                long currentTime;
                long waitInMs;
                currentTime = TimeUtil.currentTimeMillis();
                //尝试抢占下一个滑动窗口的令牌，并返回该时间窗口所剩余的时间，如果获取失败，则返回 OccupyTimeoutProperty.getOccupyTimeout() 值，该返回值的作用就是当前申请资源的线程将 sleep(阻塞)的时间。
                waitInMs = node.tryOccupyNext(currentTime, acquireCount, count);
                //如果 waitInMs 小于抢占的最大超时时间，则在下一个时间窗口中增加对应令牌数，并且线程将sleep
                if (waitInMs < OccupyTimeoutProperty.getOccupyTimeout()) {
                    node.addWaitingRequest(currentTime + waitInMs, acquireCount);
                    node.addOccupiedPass(acquireCount);
                    //这里等待的时间，默认情况下就是当前时间距离下一个时间窗口的时间，
                    sleep(waitInMs);

                    // PriorityWaitException indicates that the request will pass after waiting for {@link @waitInMs}.
                    //，在
                    /**
                     * 这里抛出PriorityWaitException异常
                     * {@link StatisticSlot#entry(com.alibaba.csp.sentinel.context.Context, com.alibaba.csp.sentinel.slotchain.ResourceWrapper, com.alibaba.csp.sentinel.node.DefaultNode, int, boolean, java.lang.Object...)}
                     * 中捕获，此种情况虽然也算通过，但是只是会增加线程数，上面node.addOccupiedPass(acquireCount)只会增加节点分钟级别通过的数量
                     */
                    throw new PriorityWaitException(waitInMs);
                }
            }
            //否则直接返回 false，最终会直接抛出 FlowException，即快速失败，应用方可以捕捉该异常，对其业务进行容错处理
            return false;
        }
        //如果当前请求的令牌数加上已消耗的令牌数之和小于总令牌数，则直接返回true，表示通过
        return true;
    }

    private int avgUsedTokens(Node node) {
        if (node == null) {
            return DEFAULT_AVG_USED_TOKENS;
        }
        return grade == RuleConstant.FLOW_GRADE_THREAD ? node.curThreadNum() : (int)(node.passQps());
    }

    private void sleep(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }
}
