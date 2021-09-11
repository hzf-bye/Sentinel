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
package com.alibaba.csp.sentinel.node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import com.alibaba.csp.sentinel.node.metric.MetricNode;
import com.alibaba.csp.sentinel.slots.statistic.metric.ArrayMetric;
import com.alibaba.csp.sentinel.slots.statistic.metric.Metric;
import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.util.function.Predicate;

/**
 * <p>The statistic node keep three kinds of real-time statistics metrics:</p>
 * <ol>
 * <li>metrics in second level ({@code rollingCounterInSecond})</li>
 * <li>metrics in minute level ({@code rollingCounterInMinute})</li>
 * <li>thread count</li>
 * </ol>
 *
 * <p>
 * Sentinel use sliding window to record and count the resource statistics in real-time.
 * The sliding window infrastructure behind the {@link ArrayMetric} is {@code LeapArray}.
 * </p>
 *
 * <p>
 * case 1: When the first request comes in, Sentinel will create a new window bucket of
 * a specified time-span to store running statics, such as total response time(rt),
 * incoming request(QPS), block request(bq), etc. And the time-span is defined by sample count.
 * </p>
 * <pre>
 * 	0      100ms
 *  +-------+--→ Sliding Windows
 * 	    ^
 * 	    |
 * 	  request
 * </pre>
 * <p>
 * Sentinel use the statics of the valid buckets to decide whether this request can be passed.
 * For example, if a rule defines that only 100 requests can be passed,
 * it will sum all qps in valid buckets, and compare it to the threshold defined in rule.
 * </p>
 *
 * <p>case 2: continuous requests</p>
 * <pre>
 *  0    100ms    200ms    300ms
 *  +-------+-------+-------+-----→ Sliding Windows
 *                      ^
 *                      |
 *                   request
 * </pre>
 *
 * <p>case 3: requests keeps coming, and previous buckets become invalid</p>
 * <pre>
 *  0    100ms    200ms	  800ms	   900ms  1000ms    1300ms
 *  +-------+-------+ ...... +-------+-------+ ...... +-------+-----→ Sliding Windows
 *                                                      ^
 *                                                      |
 *                                                    request
 * </pre>
 *
 * <p>The sliding window should become:</p>
 * <pre>
 * 300ms     800ms  900ms  1000ms  1300ms
 *  + ...... +-------+ ...... +-------+-----→ Sliding Windows
 *                                                      ^
 *                                                      |
 *                                                    request
 * </pre>
 *
 * @author qinan.qn
 * @author jialiang.linjl
 * 实现统计信息的默认实现类。
 *
 */
public class StatisticNode implements Node {

    /**
     * Holds statistics of the recent {@code INTERVAL} milliseconds. The {@code INTERVAL} is divided into time spans
     * by given {@code sampleCount}.
     * 每秒的实时统计信息，使用 ArrayMetric 实现，即基于滑动窗口实现，默认1s 采样 2次。即一个统计周期中包含两个滑动窗口。
     */
    private transient volatile Metric rollingCounterInSecond = new ArrayMetric(SampleCountProperty.SAMPLE_COUNT,
        IntervalProperty.INTERVAL);

    /**
     * Holds statistics of the recent 60 seconds. The windowLengthInMs is deliberately set to 1000 milliseconds,
     * meaning each bucket per second, in this way we can get accurate statistics of each second.
     * 每分钟实时统计信息，同样使用 ArrayMetric  实现，即基于滑动窗口实现。每1分钟，抽样60次，即包含60个滑动窗口，每一个窗口的时间间隔为 1s 。
     */
    private transient Metric rollingCounterInMinute = new ArrayMetric(60, 60 * 1000, false);

    /**
     * The counter for thread count.
     * 当前线程计数器。
     */
    private LongAdder curThreadNum = new LongAdder();

    /**
     * The last timestamp when metrics were fetched.
     * 上一次获取资源的有效统计数据的时间，即调用 Node 的 metrics() 方法的时间。
     */
    private long lastFetchTime = -1;

    /**
     * 由于 Sentienl 基于滑动窗口来实时收集统计信息，并存储在内存中，并随着时间的推移，旧的滑动窗口将失效，故需要提供一个方法，
     * 及时将所有的统计信息进行汇总输出，供监控客户端定时拉取，转储到其他客户端，例如数据库，方便监控数据的可视化，
     * 这也通常是中间件用于监控指标的监控与采集的通用设计方法。
     */
    @Override
    public Map<Long, MetricNode> metrics() {
        // The fetch operation is thread-safe under a single-thread scheduler pool.
        long currentTime = TimeUtil.currentTimeMillis();
        //
        /**
         * 获取当前时间对应的滑动窗口的开始时间。
         * 逻辑同下
         * {@link com.alibaba.csp.sentinel.slots.statistic.base.LeapArray#calculateWindowStart(long)}
         */
        currentTime = currentTime - currentTime % 1000;
        Map<Long, MetricNode> metrics = new ConcurrentHashMap<>();
        //获取一分钟内的所有滑动窗口中的统计数据，使用 MetricNode 表示。
        List<MetricNode> nodesOfEverySecond = rollingCounterInMinute.details();
        long newLastFetchTime = lastFetchTime;
        // Iterate metrics of all resources, filter valid metrics (not-empty and up-to-date).
        for (MetricNode node : nodesOfEverySecond) {
            //遍历所有节点，刷选出不是当前滑动窗口外的所有数据。这里的重点是方法：isNodeInTime。
            //这里只刷选出不是当前窗口的数据，即 metrics 方法返回的是“过去”的统计数据。
            if (isNodeInTime(node, currentTime) && isValidMetricNode(node)) {
                metrics.put(node.getTimestamp(), node);
                newLastFetchTime = Math.max(newLastFetchTime, node.getTimestamp());
            }
        }
        lastFetchTime = newLastFetchTime;

        return metrics;
    }

    @Override
    public List<MetricNode> rawMetricsInMin(Predicate<Long> timePredicate) {
        return rollingCounterInMinute.detailsOnCondition(timePredicate);
    }

    private boolean isNodeInTime(MetricNode node, long currentTime) {
        return node.getTimestamp() > lastFetchTime && node.getTimestamp() < currentTime;
    }

    private boolean isValidMetricNode(MetricNode node) {
        return node.getPassQps() > 0 || node.getBlockQps() > 0 || node.getSuccessQps() > 0
            || node.getExceptionQps() > 0 || node.getRt() > 0 || node.getOccupiedPassQps() > 0;
    }

    @Override
    public void reset() {
        rollingCounterInSecond = new ArrayMetric(SampleCountProperty.SAMPLE_COUNT, IntervalProperty.INTERVAL);
    }

    /**
     * 获取当前时间戳的总请求数，获取分钟级时间窗口中的统计信息。
     */
    @Override
    public long totalRequest() {
        return rollingCounterInMinute.pass() + rollingCounterInMinute.block();
    }

    @Override
    public long blockRequest() {
        return rollingCounterInMinute.block();
    }

    @Override
    public double blockQps() {
        return rollingCounterInSecond.block() / rollingCounterInSecond.getWindowIntervalInSec();
    }

    @Override
    public double previousBlockQps() {
        return this.rollingCounterInMinute.previousWindowBlock();
    }

    @Override
    public double previousPassQps() {
        return this.rollingCounterInMinute.previousWindowPass();
    }

    @Override
    public double totalQps() {
        return passQps() + blockQps();
    }

    @Override
    public long totalSuccess() {
        return rollingCounterInMinute.success();
    }

    @Override
    public double exceptionQps() {
        return rollingCounterInSecond.exception() / rollingCounterInSecond.getWindowIntervalInSec();
    }

    @Override
    public long totalException() {
        return rollingCounterInMinute.exception();
    }

    @Override
    public double passQps() {
        return rollingCounterInSecond.pass() / rollingCounterInSecond.getWindowIntervalInSec();
    }

    @Override
    public long totalPass() {
        return rollingCounterInMinute.pass();
    }

    /**
     * 成功TPS，用秒级统计滑动窗口中统计的个数 除以 窗口的间隔得出其 tps，即抽样个数越大，其统计越精确。
     */
    @Override
    public double successQps() {
        return rollingCounterInSecond.success() / rollingCounterInSecond.getWindowIntervalInSec();
    }

    @Override
    public double maxSuccessQps() {
        return (double) rollingCounterInSecond.maxSuccess() * rollingCounterInSecond.getSampleCount()
                / rollingCounterInSecond.getWindowIntervalInSec();
    }

    /**
     * 当前抢占未来令牌的QPS。
     */
    @Override
    public double occupiedPassQps() {
        return rollingCounterInSecond.occupiedPass() / rollingCounterInSecond.getWindowIntervalInSec();
    }

    @Override
    public double avgRt() {
        long successCount = rollingCounterInSecond.success();
        if (successCount == 0) {
            return 0;
        }

        return rollingCounterInSecond.rt() * 1.0 / successCount;
    }

    @Override
    public double minRt() {
        return rollingCounterInSecond.minRt();
    }

    @Override
    public int curThreadNum() {
        return (int)curThreadNum.sum();
    }

    /**
     * 增加通过请求数量。即将实时调用信息向滑动窗口中进行统计。
     * addPassRequest 即报告成功的通过数量。就是分别调用 秒级、分钟即对应的滑动窗口中添加数量，然后限流规则、熔断规则将基于滑动窗口中的值进行计算。
     */
    @Override
    public void addPassRequest(int count) {
        rollingCounterInSecond.addPass(count);
        rollingCounterInMinute.addPass(count);
    }

    @Override
    public void addRtAndSuccess(long rt, int successCount) {
        rollingCounterInSecond.addSuccess(successCount);
        rollingCounterInSecond.addRT(rt);

        rollingCounterInMinute.addSuccess(successCount);
        rollingCounterInMinute.addRT(rt);
    }

    @Override
    public void increaseBlockQps(int count) {
        rollingCounterInSecond.addBlock(count);
        rollingCounterInMinute.addBlock(count);
    }

    @Override
    public void increaseExceptionQps(int count) {
        rollingCounterInSecond.addException(count);
        rollingCounterInMinute.addException(count);
    }

    @Override
    public void increaseThreadNum() {
        curThreadNum.increment();
    }

    @Override
    public void decreaseThreadNum() {
        curThreadNum.decrement();
    }

    @Override
    public void debug() {
        rollingCounterInSecond.debug();
    }

    /**
     * 尝试抢占未来的令牌，返回值为调用该方法的线程应该 sleep 的时间
     * @param currentTime  current time millis. 当前时间。
     * @param acquireCount tokens count to acquire. 本次需要申请的令牌个数。
     * @param threshold    qps threshold. 设置的阔值。
     * 参考 https://www.jianshu.com/p/0da9fca33aa6
     */
    @Override
    public long tryOccupyNext(long currentTime, int acquireCount, double threshold) {
        //一个周期内允许通过的最大的令牌数量，一个周期默认值为IntervalProperty.INTERVAL / 1000s
        double maxCount = threshold * IntervalProperty.INTERVAL / 1000;
        //当前已借用的未来令牌数量
        long currentBorrow = rollingCounterInSecond.waiting();
        if (currentBorrow >= maxCount) {
            //借用失败
            return OccupyTimeoutProperty.getOccupyTimeout();
        }

        //时间窗口间隔
        int windowLength = IntervalProperty.INTERVAL / SampleCountProperty.SAMPLE_COUNT;

        /* currentTime - currentTime % windowLength + windowLength 表示 当前时间所在所在的时间窗口的结束时间
         * 再减去IntervalProperty.INTERVAL（两个时间窗口的间隔？） 表示 当前时间所在时间窗口的上一个时间窗口的开始时间？
         *     B0       B1      B2    NULL      B4
         * ||_______|_______|_______|_______|_______||___
         * 0       500     1000     1500     2000   2500  timestamp
         *          ^            ^
         *      earliestTime currentTime=1288
         *    如图currentTime=1288 那么earliestTime=500
         * 也就是earliestTime所在的时间窗口与如图currentTime所在的时间窗口合起来是一个周期，默认值为1s(IntervalProperty.INTERVAL的值)
         */
        long earliestTime = currentTime - currentTime % windowLength + windowLength - IntervalProperty.INTERVAL;

        int idx = 0;
        /*
         * Note: here {@code currentPass} may be less than it really is NOW, because time difference
         * since call rollingCounterInSecond.pass(). So in high concurrency, the following code may
         * lead more tokens be borrowed.
         */
        //currentPass指当前时间所在时间窗口所在的周期通过的令牌数
        long currentPass = rollingCounterInSecond.pass();
        /*
         * 整个方法最终结果是想得到一个等待的时间，但是该等待时间也是有最大限制的。
         * 在while循环内，earliestTime一定要小于currentTime的，currentTime比earliestTime大((SampleCountProperty.SAMPLE_COUNT-1)个窗口时间+currentTime % windowLength)这么多时间值，
         * idx表示已经遍历次数。
         */
        while (earliestTime < currentTime) {
            /*
             * 当第一次时，waitInMs的时间其实时当前时间窗口的下一个窗口开始时间减去当前时间，意思就是等待waitInMs，就是下一个窗口的开始时间了（比如currentTime=1288，windowLength=500，那么waitInMs=212）。
             * 当第二次时即idx=1，默认情况下waitInMs=windowLength+第一次的waitInMs，肯定大于OccupyTimeoutProperty.getOccupyTimeout()了。说明抢占未来令牌失败。
             */
            long waitInMs = idx * windowLength + windowLength - currentTime % windowLength;
            if (waitInMs >= OccupyTimeoutProperty.getOccupyTimeout()) {
                break;
            }
            //获取earliestTime所在窗口通过的令牌数
            long windowPass = rollingCounterInSecond.getWindowPass(earliestTime);
            /*
             * currentPass + currentBorrow + acquireCount标识当前窗口所在周期已经通过的令牌数（当前时间所在时间窗口所在的周期+借用未来的） 与 当前需要申请的令牌数（acquireCount）
             * 减去windowPass相当于减去了当前窗口的上一个窗口通过的令牌数，2个窗口合在一起算一个周期。剩下的值就是当前时间窗口通过的值 + acquireCount。
             * 所以此处就是校验如果当前时间窗口所通过的令牌数（包括占用未来的）+ acquireCount必须小于等于maxCount，也就是限制了占用未来令牌的数量。
             *
             */
            if (currentPass + currentBorrow + acquireCount - windowPass <= maxCount) {
                //如果小于那么说明是可以占用未来的令牌数的，等待waitInMs即可，
                //第一次进来waitInMs标识当前时间距离下一个时间窗口的间隔。
                return waitInMs;
            }
            //说明当前周期已经不允许占用未来令牌了。
            //earliestTime加一个时间窗口间隔，相应的currentPass减去之前earliestTime所在时间窗口的通过的令牌数。
            earliestTime += windowLength;
            currentPass -= windowPass;
            idx++;
        }

        return OccupyTimeoutProperty.getOccupyTimeout();
    }

    /**
     * 获取当前已申请的未来的令牌的个数。
     */
    @Override
    public long waiting() {
        return rollingCounterInSecond.waiting();
    }

    /**
     * 申请未来时间窗口中的令牌。
     * @param futureTime   future timestamp that the acquireCount should be added on.
     * @param acquireCount tokens count.
     */
    @Override
    public void addWaitingRequest(long futureTime, int acquireCount) {
        rollingCounterInSecond.addWaiting(futureTime, acquireCount);
    }

    /**
     * 增加申请未来令牌通过的个数。
     * @param acquireCount tokens count.
     */
    @Override
    public void addOccupiedPass(int acquireCount) {
        rollingCounterInMinute.addOccupiedPass(acquireCount);
        rollingCounterInMinute.addPass(acquireCount);
    }
}
