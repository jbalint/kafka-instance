package com.jbalint.kafka;

import java.util.ArrayList;
import java.util.Properties;
import java.util.List;
import java.util.function.Supplier;

import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import us.b3k.kafka.ws.KafkaWebsocketServer;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run the local Kafka instance
 *
 * c.f. https://gist.github.com/fjavieralba/7930018
 */
public class KafkaInstance {
    static Logger LOGGER;

    static List<Supplier<Void>> services = new ArrayList<>();

    public static void main(String args[]) throws Exception {
        PropertyConfigurator.configure(Class.class.getResourceAsStream("/log4j.properties"));

        LOGGER = LoggerFactory.getLogger(KafkaInstance.class);

        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    for (int i = services.size() - 1; i >= 0; --i) {
                        try {
                            services.get(i).get();
                        }
                        catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    try {
                        Thread.sleep(2000);
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });

        {
            Properties zkProperties = new Properties();
            zkProperties.load(Class.class.getResourceAsStream("/zklocal.properties"));
            new ZooKeeperLocal(zkProperties);
        }

        {
            Properties kafkaProperties = new Properties();
			kafkaProperties.load(Class.class.getResourceAsStream("/kafkalocal.properties"));
            KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
            KafkaServerStartable kafka = new KafkaServerStartable(kafkaConfig);
            kafka.startup();
            services.add(() -> {
                    LOGGER.info("Shutting down Kafka");
                    kafka.shutdown();
                    return null;
                });
        }

        {
            // https://github.com/confluentinc/kafka-rest/blob/master/src/main/java/io/confluent/kafkarest/KafkaRestConfig.java
            Properties restProps = new Properties();
            restProps.load(Class.class.getResourceAsStream("/kafka-rest.properties"));
            KafkaRestConfig config = new KafkaRestConfig(restProps);
            KafkaRestApplication app = new KafkaRestApplication(config);
            app.start();
            app.join();
            services.add(() -> {
                    LOGGER.warn("Shutting down kafka-rest");
                    try {
                        app.stop();
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    return null;
                });
        }

        {
            Properties wsProps = new Properties();
            wsProps.load(Class.class.getResourceAsStream("/kafka-websocket-server.properties"));
            Properties consumerProps = new Properties();
            consumerProps.load(Class.class.getResourceAsStream("/kafka-websocket-consumer.properties"));
            Properties producerProps = new Properties();
            producerProps.load(Class.class.getResourceAsStream("/kafka-websocket-producer.properties"));

            KafkaWebsocketServer server = new KafkaWebsocketServer(wsProps, consumerProps, producerProps);
            server.run();
        }
    }
}
