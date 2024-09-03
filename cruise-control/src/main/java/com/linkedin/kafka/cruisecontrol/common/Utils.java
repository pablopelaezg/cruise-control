/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.common.Cluster;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public final class Utils {

  private Utils() {

  }

  /**
   * Get topic names that match with a given topic naming pattern
   * @param topicNamePattern target topic naming pattern
   * @param topicNamesSupplier a supplier that provides topic names
   * @return topic names
   */
  public static Set<String> getTopicNamesMatchedWithPattern(Pattern topicNamePattern, Supplier<Set<String>> topicNamesSupplier) {
    if (topicNamePattern.pattern().isEmpty()) {
      return Collections.emptySet();
    }
    return topicNamesSupplier.get()
                             .stream()
                             .filter(topicName -> topicNamePattern.matcher(topicName).matches())
                             .collect(Collectors.toSet());
  }

  /**
   * Creates a Kafka cluster based on Brokers URLs
   * @param brokersUrls Brokers URLs list.
   * @return Kafka cluster based on provided list
   */
  public static Cluster getClusterFromBrokersUrls(List<String> brokersUrls) {
    List<InetSocketAddress> addresses =
            ClientUtils.parseAndValidateAddresses(brokersUrls, ClientDnsLookup.USE_ALL_DNS_IPS);
    return Cluster.bootstrap(addresses);
  }
}
