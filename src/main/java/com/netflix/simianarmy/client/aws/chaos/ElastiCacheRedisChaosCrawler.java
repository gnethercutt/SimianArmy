/*
 *
 *  Copyright 2014 Glenn Nethercutt
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.simianarmy.client.aws.chaos;

import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.elasticache.model.CacheCluster;
import com.amazonaws.services.elasticache.model.CacheNode;
import com.netflix.simianarmy.GroupType;
import com.netflix.simianarmy.basic.chaos.BasicInstanceGroup;
import com.netflix.simianarmy.chaos.ChaosCrawler;
import com.netflix.simianarmy.client.aws.AWSClient;

/**
 * The Class ElastiCacheRedisCrawler. This will crawl for all available Redis ElastiCache clusters associated with the AWS account.
 */
public class ElastiCacheRedisChaosCrawler implements ChaosCrawler {

    /**
     * The group types Types.
     */
    public enum Types implements GroupType {

        /** only crawls ElastiCache */
        ElastiCache;
    }

    /** The aws client. */
    private final AWSClient awsClient;

    /**
     * Instantiates a new ElastiCacheRedis chaos crawler.
     * @param awsClient
     *            the aws client
     * 
     */
    public ElastiCacheRedisChaosCrawler(AWSClient awsClient) {
        this.awsClient = awsClient;
    }

    /** {@inheritDoc} */
    @Override
    public EnumSet<?> groupTypes() {
        return EnumSet.allOf(Types.class);
    }

    /** {@inheritDoc} */
    @Override
    public List<InstanceGroup> groups() {
        return groups((String[]) null);
    }

    @Override
    public List<InstanceGroup> groups(String... names) {
        List<InstanceGroup> list = new LinkedList<InstanceGroup>();
        for (CacheCluster cluster : awsClient.describeElastiCacheClusters()) {
            if (cluster.getEngine().equalsIgnoreCase("redis") &&
                    cluster.getCacheClusterStatus().equalsIgnoreCase("available")) {
                InstanceGroup ig = new BasicInstanceGroup(cluster.getCacheClusterId(), Types.ElastiCache, awsClient.region());
                for (CacheNode node : cluster.getCacheNodes()) {
                    ig.addInstance(node.getEndpoint().getAddress());
                }
                list.add(ig);
            }
        }
        return list;
    }
}
