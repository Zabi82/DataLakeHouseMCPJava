package com.mcp.datalakehouse.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class FlinkToolService {
    private static final Logger logger = LoggerFactory.getLogger(FlinkToolService.class);
    private static final Pattern JOB_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");

    @Value("${flink.rest.url}")
    private String flinkRestUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    @Tool(
        name = "flink_overview",
        description = "Get Flink cluster overview metrics such as number of task managers, slots available, jobs running, jobs finished, jobs cancelled, and jobs failed from the REST API."
    )
    public Object getFlinkOverview() {
        try {
            Object result = restTemplate.getForObject(flinkRestUrl + "/overview", Object.class);
            logger.info("Flink overview fetched: {}", result);
            return result;
        } catch (RestClientException e) {
            logger.error("Error fetching Flink overview: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(
        name = "flink_jobmanager_metrics",
        description = "Get Flink JobManager metrics such as heap memory usage, CPU load, and other JVM/process stats from the REST API."
    )
    public Object getFlinkJobManagerMetrics() {
        try {
            Object result = restTemplate.getForObject(flinkRestUrl + "/jobmanager/metrics", Object.class);
            logger.info("Flink JobManager metrics fetched: {}", result);
            return result;
        } catch (RestClientException e) {
            logger.error("Error fetching Flink JobManager metrics: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(
        name = "flink_taskmanagers_metrics",
        description = "Get Flink TaskManagers metrics such as heap memory usage, network IO, and task slot utilization from the REST API."
    )
    public Object getFlinkTaskManagersMetrics() {
        try {
            Object result = restTemplate.getForObject(flinkRestUrl + "/taskmanagers/metrics", Object.class);
            logger.info("Flink TaskManagers metrics fetched: {}", result);
            return result;
        } catch (RestClientException e) {
            logger.error("Error fetching Flink TaskManagers metrics: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(
        name = "flink_jobs",
        description = "List all Flink jobs running on the cluster, including job IDs, names, and status."
    )
    public Object getFlinkJobs() {
        try {
            Object result = restTemplate.getForObject(flinkRestUrl + "/jobs", Object.class);
            logger.info("Flink jobs fetched: {}", result);
            return result;
        } catch (RestClientException e) {
            logger.error("Error fetching Flink jobs: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(
        name = "flink_job_details",
        description = "Get details for a list of Flink jobs by job ID(s), including status, vertices, and configuration. Accepts a 'job_ids' list parameter."
    )
    public Object getFlinkJobDetails(List<String> jobIds) {
        if (jobIds == null || jobIds.isEmpty()) {
            return Map.of("error", "job_ids must be a non-empty list of job IDs");
        }

        Map<String, Object> results = new HashMap<>();

        for (String jobId : jobIds) {
            try {
                String sanitizedId = sanitizeJobId(jobId);
                Object result = restTemplate.getForObject(flinkRestUrl + "/jobs/" + sanitizedId, Object.class);
                logger.info("Flink job details for '{}': {}", sanitizedId, result);
                results.put(jobId, result);
            } catch (IllegalArgumentException e) {
                logger.error("Invalid job_id '{}': {}", jobId, e.getMessage());
                results.put(jobId, Map.of("error", e.getMessage()));
            } catch (RestClientException e) {
                logger.error("Error fetching Flink job details for '{}': {}", jobId, e.getMessage());
                results.put(jobId, Map.of("error", e.getMessage()));
            }
        }

        return results;
    }

    @Tool(
        name = "probe_jobmanager_metrics",
        description = "Probe one or more JobManager metrics by name from the Flink REST API. Accepts a list of metric names (can be a single metric in a list)."
    )
    public Object probeJobManagerMetrics(List<String> metricNames) {
        if (metricNames == null || metricNames.isEmpty()) {
            return Map.of("error", "metric_names must be a non-empty list");
        }

        try {
            String metricsQuery = metricNames.stream()
                    .map(name -> "get=" + name)
                    .collect(Collectors.joining("&"));

            Object result = restTemplate.getForObject(
                    flinkRestUrl + "/jobmanager/metrics?" + metricsQuery,
                    Object.class
            );

            logger.info("Probed JobManager metrics '{}': {}", metricNames, result);
            return Map.of("metrics", result);
        } catch (RestClientException e) {
            logger.error("Error probing JobManager metrics '{}': {}", metricNames, e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(
        name = "probe_taskmanager_metrics",
        description = "Probe one or more TaskManager metrics by name from the Flink REST API. Accepts TaskManager ID and a list of metric names (can be a single metric in a list)."
    )
    public Object probeTaskManagerMetrics(String taskmanagerId, List<String> metricNames) {
        if (taskmanagerId == null || taskmanagerId.trim().isEmpty()) {
            return Map.of("error", "taskmanager_id is required");
        }

        if (metricNames == null || metricNames.isEmpty()) {
            return Map.of("error", "metric_names must be a non-empty list");
        }

        try {
            String metricsQuery = metricNames.stream()
                    .map(name -> "get=" + name)
                    .collect(Collectors.joining("&"));

            Object result = restTemplate.getForObject(
                    flinkRestUrl + "/taskmanagers/" + taskmanagerId + "/metrics?" + metricsQuery,
                    Object.class
            );

            logger.info("Probed TaskManager '{}' metrics '{}': {}", taskmanagerId, metricNames, result);
            return Map.of("metrics", result);
        } catch (RestClientException e) {
            logger.error("Error probing TaskManager '{}' metrics '{}': {}", taskmanagerId, metricNames, e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(
        name = "flink_taskmanagers",
        description = "List all Flink TaskManagers and their details from the REST API."
    )
    public Object getFlinkTaskManagers() {
        try {
            Object result = restTemplate.getForObject(flinkRestUrl + "/taskmanagers", Object.class);
            logger.info("Flink TaskManagers fetched: {}", result);
            return result;
        } catch (RestClientException e) {
            logger.error("Error fetching Flink TaskManagers: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    private String sanitizeJobId(String jobId) {
        if (jobId == null || !JOB_ID_PATTERN.matcher(jobId).matches()) {
            throw new IllegalArgumentException("Invalid job_id: " + jobId);
        }
        return jobId;
    }
}
