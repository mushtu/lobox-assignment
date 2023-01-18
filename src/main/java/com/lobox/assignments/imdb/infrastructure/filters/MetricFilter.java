package com.lobox.assignments.imdb.infrastructure.filters;

import com.lobox.assignments.imdb.application.services.MetricService;
import jakarta.servlet.*;
import org.springframework.stereotype.Component;

import java.io.IOException;


@Component
public class MetricFilter implements Filter {
    private final MetricService metricService;

    public MetricFilter(MetricService metricService) {
        this.metricService = metricService;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        chain.doFilter(request, response);
        metricService.incrementHttpRequestCounter();
    }
}
