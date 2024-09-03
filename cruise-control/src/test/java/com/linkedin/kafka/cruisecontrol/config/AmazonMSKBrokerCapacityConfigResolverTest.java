/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.aws.AWSClient;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AmazonMSKBrokerCapacityConfigResolverTest {

    private AWSClient _awsClientMock;
    private ScheduledExecutorService _scheduledExecutorServiceMock;
    private AmazonMSKBrokerCapacityConfigResolver _resolver;

    /**
     * Setup for all tests
     */
    @Before
    public void setUp() {
        _awsClientMock = createMock(AWSClient.class);
        _scheduledExecutorServiceMock = createMock(ScheduledExecutorService.class);
        _resolver = new AmazonMSKBrokerCapacityConfigResolver() {
            @Override
            protected AWSClient getAWSClient(String clusterArn, String clusterRegion, double cpuRate, double incomingNetworkRate) {
                return _awsClientMock;
            }

            @Override
            protected ScheduledExecutorService getCapacityRefresher(Integer refreshPeriodMinutes) {
                return _scheduledExecutorServiceMock;
            }
        };
    }

    @Test
    public void testCapacityForBrokerActiveStatus() throws TimeoutException, BrokerCapacityResolutionException {
        BrokerCapacityInfo expectedCapacityInfo = brokerCapacityInfo();
        expect(_awsClientMock.getBrokerCapacityInfo()).andReturn(Pair.of(ClusterState.ACTIVE, expectedCapacityInfo));
        replay(_awsClientMock, _scheduledExecutorServiceMock);

        _resolver.configure(getConfigMap());
        _resolver.refreshCapacityInfo();
        BrokerCapacityInfo capacityInfo = _resolver.capacityForBroker("rack1", "host1", 1, 1000L, true);

        assertEquals(expectedCapacityInfo, capacityInfo);
    }

    @Test
    public void testCapacityForBrokerNoRefresh() throws TimeoutException, BrokerCapacityResolutionException {
        BrokerCapacityInfo expectedCapacityInfo = brokerCapacityInfo();
        expect(_awsClientMock.getBrokerCapacityInfo()).andReturn(Pair.of(ClusterState.UPDATING, expectedCapacityInfo));
        replay(_awsClientMock, _scheduledExecutorServiceMock);

        _resolver.configure(getConfigMap());
        _resolver.refreshCapacityInfo();
        BrokerCapacityInfo capacityInfo = _resolver.capacityForBroker("rack1", "host1", 1,
                1000L, true);

        assertNull(capacityInfo);
    }

    @Test
    public void testCapacityForBrokerThrowsException() throws TimeoutException, BrokerCapacityResolutionException {
        expect(_awsClientMock.getBrokerCapacityInfo()).andThrow(
                AwsServiceException.builder().message("Service Exception").build());
        replay(_awsClientMock, _scheduledExecutorServiceMock);

        _resolver.configure(getConfigMap());
        _resolver.refreshCapacityInfo();
        assertNull(_resolver.capacityForBroker("rack1", "host1", 1,
                1000L, true));
    }

    private static BrokerCapacityInfo brokerCapacityInfo() {
        return new BrokerCapacityInfo(Map.of(
                Resource.CPU, 100.0,
                Resource.DISK, 100.0,
                Resource.NW_IN, 10000.0,
                Resource.NW_OUT, 10000.0
        ));
    }

    private Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(MonitorConfig.MSK_CLUSTER_ARN, "arn:aws:kafka:region:account:cluster/clusterName");
        configMap.put(MonitorConfig.MSK_CLUSTER_REGION, "region");
        configMap.put(MonitorConfig.MSK_CPU_CAPACITY_RATIO, 1.0);
        configMap.put(MonitorConfig.MSK_NETWORK_INBOUND_TRAFFIC_RATIO, 0.5);
        configMap.put(MonitorConfig.BROKER_CAPACITY_CONFIG_RESOLVER_AWS_FETCH_PERIOD_MINUTES, 5);
        return configMap;
    }
}
