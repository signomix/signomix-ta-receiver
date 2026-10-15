package com.signomix.receiver.adapter.in;

import com.signomix.receiver.ReceiverService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

/**
 * REST endpoint for monitoring memory usage and cache statistics.
 * Useful for detecting and diagnosing memory leaks in production.
 */
@Path("/api/memory")
public class MemoryMonitorResource {

    @Inject
    Logger LOG;

    @Inject
    ReceiverService receiverService;

    /**
     * Returns comprehensive memory statistics including:
     * - JVM heap usage
     * - Frame counters cache statistics
     * - System memory information
     */
    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMemoryStatus() {
        Runtime runtime = Runtime.getRuntime();

        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = allocatedMemory - freeMemory;

        double usagePercentage = (maxMemory > 0) ? (usedMemory * 100.0 / maxMemory) : 0;

        // Get frame counters cache statistics if available
        long frameCountersSize = 0;
        try {
            // Access the cache via reflection to get statistics
            // This is safe as we control the ReceiverService
            frameCountersSize = receiverService.getFrameCountersCacheSize();
        } catch (Exception e) {
            LOG.debug("Could not get frame counters cache size: " + e.getMessage());
        }

        MemoryStatus status = new MemoryStatus();
        status.maxMemory = maxMemory;
        status.allocatedMemory = allocatedMemory;
        status.freeMemory = freeMemory;
        status.usedMemory = usedMemory;
        status.usagePercentage = String.format("%.2f%%", usagePercentage);
        status.frameCountersCacheSize = frameCountersSize;
        status.threadCount = Thread.activeCount();

        // Add memory recommendations
        if (usagePercentage > 80) {
            status.warning = "High memory usage detected (>80%). Consider increasing heap size.";
        } else if (usagePercentage > 90) {
            status.warning = "Critical memory usage detected (>90%). Immediate action required.";
        } else {
            status.warning = "Memory usage is normal.";
        }

        return Response.ok(status).build();
    }

    /**
     * Returns a simple health check for memory subsystem.
     */
    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMemoryHealth() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double usagePercentage = (maxMemory > 0) ? (usedMemory * 100.0 / maxMemory) : 0;

        MemoryHealth health = new MemoryHealth();
        health.healthy = usagePercentage < 85; // 85% threshold
        health.usagePercentage = String.format("%.2f%%", usagePercentage);
        health.usedMemory = usedMemory;
        health.maxMemory = maxMemory;

        int statusCode = health.healthy ? 200 : 503;
        return Response.status(statusCode).entity(health).build();
    }

    /**
     * Triggers garbage collection and returns memory stats.
     * Useful for debugging but should not be called in production.
     */
    @GET
    @Path("/gc")
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerGarbageCollection() {
        System.gc();

        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double usagePercentage = (maxMemory > 0) ? (usedMemory * 100.0 / maxMemory) : 0;

        MemoryStatus status = new MemoryStatus();
        status.maxMemory = maxMemory;
        status.usedMemory = usedMemory;
        status.usagePercentage = String.format("%.2f%%", usagePercentage);
        status.message = "Garbage collection triggered";

        return Response.ok(status).build();
    }

    /**
     * Memory status DTO
     */
    public static class MemoryStatus {
        public long maxMemory;
        public long allocatedMemory;
        public long freeMemory;
        public long usedMemory;
        public String usagePercentage;
        public long frameCountersCacheSize;
        public int threadCount;
        public String warning;
        public String message;
    }

    /**
     * Memory health DTO
     */
    public static class MemoryHealth {
        public boolean healthy;
        public String usagePercentage;
        public long usedMemory;
        public long maxMemory;
    }
}
