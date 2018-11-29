package com.github.barynx.kafkaJMX;

import com.yammer.metrics.reporting.JmxReporter.HistogramMBean;
import com.yammer.metrics.reporting.JmxReporter.GaugeMBean;
import com.yammer.metrics.reporting.JmxReporter.MeterMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.util.Calendar;


public class KafkaJMXMonitor {

    Logger logger = LoggerFactory.getLogger(KafkaJMXMonitor.class.getName());

    public static void main(String[] args)  {

        String hostname = "3.120.31.41";
        String port = "9999";
        long pollingIntervalMs = 5000;
        String csvOutputPath = "./KafkaMetrics.csv";

            new KafkaJMXMonitor().run(hostname, port, pollingIntervalMs);
    }

    public void run(String hostname, String port, long pollingIntervalMs){

        try {
            //connect to JMX server
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi");
            JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
            MBeanServerConnection mbeanServerConnection = jmxConnector.getMBeanServerConnection();

            //specify objects to query
            ObjectName bytesInPerSec = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
            ObjectName bytesOutPerSec = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec");
            ObjectName underReplicatedPartitions = new ObjectName("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions");
            ObjectName activeControllerCount = new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount");
            ObjectName offlinePartitionsCount = new ObjectName("kafka.controller:type=KafkaController,name=OfflinePartitionsCount");
            ObjectName totalTimeMsRequestProduce = new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce");
            ObjectName totalTimeMsRequestFetchConsumer = new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer");

            //set up proxy instances for queried MBeans
            MeterMBean bytesInPerSecMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, bytesInPerSec, MeterMBean.class, true);
            MeterMBean bytesOutPerSecMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, bytesOutPerSec, MeterMBean.class, true);

            GaugeMBean underReplicatedPartitionsMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, underReplicatedPartitions, GaugeMBean.class, true);
            GaugeMBean activeControllerCountMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, activeControllerCount, GaugeMBean.class, true);
            GaugeMBean offlinePartitionsCountMbeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, offlinePartitionsCount, GaugeMBean.class, true);

            HistogramMBean totalTimeMsRequestProduceMBeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, totalTimeMsRequestProduce, HistogramMBean.class, true);
            HistogramMBean totalTimeMsRequestFetchConsumerMBeanProxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServerConnection, totalTimeMsRequestFetchConsumer, HistogramMBean.class, true);

            //query JMX server every pollingIntervalMs milliseconds
            while(true){

                logger.info("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions: " + underReplicatedPartitionsMbeanProxy.getValue());
                logger.info("kafka.server:type=ReplicaManager,name=ActiveControllerCount: " + activeControllerCountMbeanProxy.getValue());
                logger.info("kafka.server:type=ReplicaManager,name=OfflinePartitionsCount: " + offlinePartitionsCountMbeanProxy.getValue());

                logger.info("Current time: " + Calendar.getInstance().getTime());
                logger.info("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec: " + bytesInPerSecMbeanProxy.getOneMinuteRate());
                logger.info("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec: " + bytesOutPerSecMbeanProxy.getOneMinuteRate());

                logger.info("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce (mean): " + totalTimeMsRequestProduceMBeanProxy.getMean());
                logger.info("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer (mean): " + totalTimeMsRequestFetchConsumerMBeanProxy.getMean());

                //append row to CSV

                Thread.sleep(pollingIntervalMs);
            }

        } catch (IOException e) {
            logger.error("Connection failed", e);
        } catch (MalformedObjectNameException e) {
            logger.error("Incorrect name of MBean", e);
        } catch (InterruptedException e) {
            logger.error("Something wrong with thread sleep", e);
        }
    }
}

