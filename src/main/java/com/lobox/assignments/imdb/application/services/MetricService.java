package com.lobox.assignments.imdb.application.services;

import com.lobox.assignments.imdb.application.dto.Metric;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.LongAdder;

@Service
public class MetricService {

    private final LongAdder httpRequestsCounter;

    public MetricService() {
        this.httpRequestsCounter = new LongAdder();
    }

    public Metric getMetrics() {
        return new Metric(httpRequestsCounter.sum());
    }

    public void incrementHttpRequestCounter() {
        httpRequestsCounter.increment();
    }


}
