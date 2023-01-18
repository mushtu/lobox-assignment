package com.lobox.assignments.imdb.api;

import com.lobox.assignments.imdb.application.dto.Metric;
import com.lobox.assignments.imdb.application.services.MetricService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetricController {
    private final MetricService metricService;

    public MetricController(MetricService metricService) {
        this.metricService = metricService;
    }

    @GetMapping(value = "/metric")
    public Metric getMetrics() {
        return metricService.getMetrics();
    }
}
