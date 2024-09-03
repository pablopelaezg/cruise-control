/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.aws;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;
import software.amazon.awssdk.services.ec2.model.NetworkInfo;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.BrokerNodeGroupInfo;
import software.amazon.awssdk.services.kafka.model.ClusterInfo;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DescribeClusterRequest;
import software.amazon.awssdk.services.kafka.model.DescribeClusterResponse;
import software.amazon.awssdk.services.kafka.model.EBSStorageInfo;
import software.amazon.awssdk.services.kafka.model.StorageInfo;
import java.util.Map;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;

public class AWSClientTest {

    private static final String TEST_CLUSTER_ARN = "arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/a25ac233-1303-4040-a9b3-6fd4092aaccd-1";
    private static final String TEST_INSTANCE_TYPE = "t3.small";
    private static final double TEST_CPU_RATIO = 1.0;
    private static final double TEST_INCOMING_NETWORK_RATIO = 0.5;

    @Test
    public void testGetBrokerCapacityInfo() {

        KafkaClient mockKafkaClient = mock(KafkaClient.class);
        EasyMock.expect(mockKafkaClient.describeCluster(describeClusterRequestMatches(TEST_CLUSTER_ARN)))
                .andReturn(describeClusterResponse());

        Ec2Client mockEc2Client = mock(Ec2Client.class);
        EasyMock.expect(mockEc2Client.describeInstanceTypes(describeInstanceTypesRequest(TEST_INSTANCE_TYPE)))
                .andReturn(describeInstanceTypesResponse());

        EasyMock.replay(mockKafkaClient, mockEc2Client);

        AWSClient awsClient = new AWSClient(TEST_CLUSTER_ARN, mockKafkaClient, mockEc2Client,
                TEST_CPU_RATIO, TEST_INCOMING_NETWORK_RATIO);
        Pair<ClusterState, BrokerCapacityInfo> result = awsClient.getBrokerCapacityInfo();

        EasyMock.verify(mockKafkaClient, mockEc2Client);

        assert (result != null);
        assertEquals(result.getLeft(), ClusterState.ACTIVE);
        assert (result.getRight() != null);
        assert (result.getRight().capacity() != null);
        assert (!result.getRight().capacity().isEmpty());

        Map<Resource, Double> capacity = result.getRight().capacity();
        assertEquals(Double.valueOf(100.0), capacity.get(Resource.CPU));
        assertEquals(Double.valueOf(100.0), capacity.get(Resource.DISK));
        assertEquals(Double.valueOf(5000000.0), capacity.get(Resource.NW_IN));
        assertEquals(Double.valueOf(5000000.0), capacity.get(Resource.NW_OUT));
    }

    @Test(expected = AwsServiceException.class)
    public void testGetBrokerCapacityInfoErrorClient() {

        KafkaClient mockKafkaClient = mock(KafkaClient.class);
        EasyMock.expect(mockKafkaClient.describeCluster(describeClusterRequestMatches(TEST_CLUSTER_ARN)))
                .andReturn(describeClusterResponse());

        Ec2Client mockEc2Client = mock(Ec2Client.class);
        EasyMock.expect(mockEc2Client.describeInstanceTypes(describeInstanceTypesRequest(TEST_INSTANCE_TYPE)))
                .andThrow(AwsServiceException.builder().message("Test").build());

        EasyMock.replay(mockKafkaClient, mockEc2Client);

        AWSClient awsClient = new AWSClient(TEST_CLUSTER_ARN, mockKafkaClient, mockEc2Client,
                TEST_CPU_RATIO, TEST_INCOMING_NETWORK_RATIO);
        awsClient.getBrokerCapacityInfo();
    }

    private static DescribeClusterResponse describeClusterResponse() {
        return DescribeClusterResponse.builder().clusterInfo(
                ClusterInfo.builder()
                        .state("ACTIVE")
                        .brokerNodeGroupInfo(BrokerNodeGroupInfo.builder()
                                .instanceType("kafka." + TEST_INSTANCE_TYPE)
                                .storageInfo(StorageInfo.builder()
                                        .ebsStorageInfo(
                                                EBSStorageInfo
                                                        .builder()
                                                        .volumeSize(100)
                                                        .build())
                                        .build())
                                .build()).build()).build();
    }

    private static DescribeInstanceTypesResponse describeInstanceTypesResponse() {
        return DescribeInstanceTypesResponse.builder()
                .instanceTypes(
                        InstanceTypeInfo
                                .builder()
                                .networkInfo(NetworkInfo.builder()
                                        .networkPerformance("Up to 10 Gigabit")
                                        .build())
                        .build())
                .build();
    }

    private static DescribeClusterRequest describeClusterRequestMatches(String clusterArn) {
        EasyMock.reportMatcher(new IArgumentMatcher() {
            @Override
            public boolean matches(Object argument) {
                if (argument instanceof DescribeClusterRequest) {
                    DescribeClusterRequest request = (DescribeClusterRequest) argument;
                    return clusterArn.equals(request.clusterArn());
                }
                return false;
            }

            @Override
            public void appendTo(StringBuffer buffer) {
                buffer.append("Expected ARN: ").append(clusterArn);
            }
        });
        return null;
    }

    private static DescribeInstanceTypesRequest describeInstanceTypesRequest(String instanceType) {
        EasyMock.reportMatcher(new IArgumentMatcher() {
            @Override
            public boolean matches(Object argument) {
                if (argument instanceof DescribeInstanceTypesRequest) {
                    DescribeInstanceTypesRequest request = (DescribeInstanceTypesRequest) argument;
                    return request.instanceTypesAsStrings().contains(instanceType);
                }
                return false;
            }

            @Override
            public void appendTo(StringBuffer buffer) {
                buffer.append("Expected Instance Type: ").append(instanceType);
            }
        });
        return null;
    }

}
