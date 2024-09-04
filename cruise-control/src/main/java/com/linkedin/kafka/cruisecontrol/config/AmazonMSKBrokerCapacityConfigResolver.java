/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.aws.AWSClient;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class AmazonMSKBrokerCapacityConfigResolver implements BrokerCapacityConfigResolver {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonMSKBrokerCapacityConfigResolver.class);
    private AWSClient _awsClient;
    private final AtomicReference<BrokerCapacityInfo> _capacityInfo = new AtomicReference<>(null);
    private ScheduledExecutorService _capacityRefresher;

    @Override
    public BrokerCapacityInfo capacityForBroker(String rack,
                                                String host,
                                                int brokerId,
                                                long timeoutMs,
                                                boolean allowCapacityEstimation)
            throws TimeoutException, BrokerCapacityResolutionException {

        LOG.info("Querying cached AWS MSK Capacity Info: {}", _capacityInfo.get() == null ? "EMPTY"
                : _capacityInfo.get().capacity());
        return _capacityInfo.get();
    }

    @Override
    public void configure(Map<String, ?> configs) {

        String clusterArn = Optional.of((String) configs.get(MonitorConfig.MSK_CLUSTER_ARN))
                .filter(str -> !str.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("AWS Broker Data Source is enabled, "
                        + "so AWS MSK cluster ARN is not allowed to be empty"));
        String clusterRegion = Optional.of((String) configs.get(MonitorConfig.MSK_CLUSTER_REGION))
                .filter(str -> !str.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("AWS Broker Data Source is enabled, "
                        + "so AWS MSK cluster region is not allowed to be empty"));
        Double cpuRatio = ((Double) configs.get(MonitorConfig.MSK_CPU_CAPACITY_RATIO));
        Double incomingNetworkRatio = ((Double) configs.get(MonitorConfig.MSK_NETWORK_INBOUND_TRAFFIC_RATIO));

        _awsClient = getAWSClient(clusterArn, clusterRegion, cpuRatio, incomingNetworkRatio);
        Integer refreshPeriodMinutes = (Integer) configs.get(
                MonitorConfig.BROKER_CAPACITY_CONFIG_RESOLVER_AWS_FETCH_PERIOD_MINUTES);
        _capacityRefresher = getCapacityRefresher(refreshPeriodMinutes);
    }

    protected void refreshCapacityInfo() {
        try {
            var awsInfo = _awsClient.getBrokerCapacityInfo();

            if (ClusterState.ACTIVE.equals(awsInfo.getLeft())) {
                _capacityInfo.set(awsInfo.getRight());
                LOG.info("AWS MSK capacity info successfully refreshed");
            }
        } catch (AwsServiceException awsException) {
            LOG.error("Failed to get Broker Capacity Info", awsException);
        }
    }

    protected AWSClient getAWSClient(String clusterArn, String clusterRegion, double cpuRatio, double incomingNetworkRatio) {
        return new AWSClient(clusterArn, clusterRegion, cpuRatio, incomingNetworkRatio);
    }

    protected ScheduledExecutorService getCapacityRefresher(Integer refreshPeriodMinutes) {
        LOG.info("Creating Broker capacity refresher executor. Refresh period: {} minutes", refreshPeriodMinutes);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this::refreshCapacityInfo, 0L, refreshPeriodMinutes, TimeUnit.MINUTES);
        return executorService;
    }

    @Override
    public void close() throws Exception {
        if (_awsClient != null) {
            _awsClient.close();
        }
        if (_capacityRefresher != null) {
            _capacityRefresher.shutdown();
        }
    }

}
