/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.aws;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DescribeClusterRequest;
import software.amazon.awssdk.services.kafka.model.DescribeClusterResponse;
import java.util.HashMap;
import java.util.Map;

public class AWSClient implements AutoCloseable {

    private final KafkaClient _kafkaClient;
    private final Ec2Client _ec2Client;

    private final String _clusterArn;
    private final double _cpuRatio;
    private final double _incomingNetworkRatio;

    public AWSClient(String clusterArn, String region, double cpuRatio, double incomingNetworkRatio) {
        this(clusterArn,
                KafkaClient.builder().region(Region.of(region)).build(),
                Ec2Client.builder().region(Region.of(region)).build(), cpuRatio, incomingNetworkRatio);
    }

    //Protected constructor for Testing
    protected AWSClient(String clusterArn, KafkaClient kafkaClient, Ec2Client ec2Client, double cpuRatio, double incomingNetworkRatio) {
        _clusterArn = clusterArn;
        _kafkaClient = kafkaClient;
        _ec2Client = ec2Client;
        _cpuRatio = cpuRatio;
        _incomingNetworkRatio = incomingNetworkRatio;

    }

    /**
     * Fetches the MSK cluster state and capacity info
     * @return Pair object with State (left) and Capacity Info (right)
     */
    public Pair<ClusterState, BrokerCapacityInfo> getBrokerCapacityInfo() {

        DescribeClusterRequest describeClusterRequest = DescribeClusterRequest.builder()
                .clusterArn(_clusterArn)
                .build();

        DescribeClusterResponse describeClusterResponse = _kafkaClient.describeCluster(describeClusterRequest);
        String instanceType = getEC2InstanceType(describeClusterResponse);

        DescribeInstanceTypesRequest request = DescribeInstanceTypesRequest.builder()
                .instanceTypes(InstanceType.fromValue(instanceType))
                .build();

        DescribeInstanceTypesResponse response = _ec2Client.describeInstanceTypes(request);
        InstanceTypeInfo instanceTypeInfo = response.instanceTypes().get(0);
        double networkPerformance = parseNetworkPerformance(instanceTypeInfo.networkInfo().networkPerformance());

        Map<Resource, Double> capacities = new HashMap<>();
        capacities.put(Resource.CPU, _cpuRatio * 100);
        capacities.put(Resource.DISK, getDiskCapacity(describeClusterResponse));
        capacities.put(Resource.NW_IN, networkPerformance * _incomingNetworkRatio);
        capacities.put(Resource.NW_OUT, networkPerformance * (1 - _incomingNetworkRatio));

        return Pair.of(describeClusterResponse.clusterInfo().state(), new BrokerCapacityInfo(capacities));
    }

    private Double getDiskCapacity(DescribeClusterResponse describeClusterResponse) {
        return describeClusterResponse.clusterInfo().brokerNodeGroupInfo().storageInfo().ebsStorageInfo().volumeSize().doubleValue() * 1000;
    }

    private String getEC2InstanceType(DescribeClusterResponse describeClusterResponse) {
        return describeClusterResponse.clusterInfo().brokerNodeGroupInfo().instanceType().replace("kafka.", "");
    }

    private double parseNetworkPerformance(String networkPerformance) {
        if (networkPerformance != null) {
            if (networkPerformance.contains("Gigabit")) {
                return Double.parseDouble(networkPerformance.replaceAll("[^\\d.]", "")) * 1000000;
            } else if (networkPerformance.contains("Megabit")) {
                return Double.parseDouble(networkPerformance.replaceAll("[^\\d.]", "")) * 1000;
            }
        }
        return 0.0;
    }

    @Override
    public void close() throws Exception {
        _kafkaClient.close();
        _ec2Client.close();
    }
}
