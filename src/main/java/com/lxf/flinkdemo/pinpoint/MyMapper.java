package com.lxf.flinkdemo.pinpoint;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;

/**
 * @author created by LiuXF on 2023/12/01 17:34:59
 */
public class MyMapper extends RichMapFunction<Long, Long> {
    private transient Histogram histogram;

    @Override
    public void open(Configuration config) {
        com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

        this.histogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram)).getDropwizardHistogram();
    }

    @Override
    public Long map(Long value) throws Exception {
        this.histogram.update(value);
        return value;
    }
}
