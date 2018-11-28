package com.github.barynx.kafkaJMX;

import com.yammer.metrics.reporting.JmxReporter.GaugeMBean;
import com.yammer.metrics.reporting.JmxReporter.MeterMBean;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class KafkaJMXMonitor {

    public static void main(String[] args) throws IOException, MalformedObjectNameException, InterruptedException {



        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + "3.120.31.41" + ":" + "9999" + "/jmxrmi");
        JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
        MBeanServerConnection mbeanServerConnection = jmxConnector.getMBeanServerConnection();
        ObjectName bytesInPerSec = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
        ObjectName bytesOutPerSec = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec");
        ObjectName underReplicatedPartitions = new ObjectName("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions");
        ObjectName isrShrinksPerSec = new ObjectName("kafka.server:type=ReplicaManager,name=IsrShrinksPerSec");
        ObjectName activeControllerCount = new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount");
        ObjectName offlinePartitionsCount = new ObjectName("kafka.controller:type=KafkaController,name=OfflinePartitionsCount");
        ObjectName leaderElectionRateAndTimeMs = new ObjectName("kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs");
        ObjectName uncleanLeaderElectionsPerSec = new ObjectName("kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec");
        ObjectName totalTimeMsRequestProduce = new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce");
        ObjectName totalTimeMsRequestFetchConsumer = new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer");
        ObjectName totalTimeMsRequestFetchFollower = new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower");


        MeterMBean bytesInPerSecMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, bytesInPerSec, MeterMBean.class, true);
        MeterMBean bytesOutPerSecMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, bytesOutPerSec, MeterMBean.class, true);
        GaugeMBean underReplicatedPartitionsMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, underReplicatedPartitions, GaugeMBean.class, true);

        while(true){

            System.out.println("Current time " + System.currentTimeMillis());
            System.out.println("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec: " + bytesInPerSecMbeanProxy.getOneMinuteRate());
            System.out.println("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec: " + bytesOutPerSecMbeanProxy.getOneMinuteRate());
            System.out.println("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions: " + underReplicatedPartitionsMbeanProxy.getValue());
            TimeUnit.SECONDS.sleep(60);
        }

        //jmxConnector.close();
    }
}

