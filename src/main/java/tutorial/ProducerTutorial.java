/**
 * Copyright 2017 Streamlio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tutorial;

import java.io.IOException;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
// import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
// import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;import java.util.Arrays;


public class ProducerTutorial {

    private static final String SERVICE_URL = "pulsar://localhost:6650";

    private static final String TOPIC_NAME = "persistent://sample/standalone/ns1/tutorial-topic";

    public static void main(String[] args) throws IOException {
        // Create a Pulsar client instance. A single instance can be shared across many
        // producers and consumer within the same application
        // PulsarClient client = PulsarClient.create(SERVICE_URL);
        PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

        // Here you get the chance to configure producer specific settings. eg:
        // ProducerConfiguration conf = new ProducerConfiguration();

        // Enable compression
        // conf.setCompressionType(CompressionType.LZ4);

        // Once the producer is created, it can be used for the entire application life-cycle
        // Producer producer = client.createProducer(TOPIC_NAME, conf);
        // log.info("Created Pulsar producer");

        List<String> restrictReplicationTo = Arrays.asList(
            "c1",
            "c2",
            "c3"
        );

        Producer producer = client.newProducer()
        .topic("non-persistent://my-tenant/my-namespace/wana")
        .maxPendingMessages(10000)
        .create();
        log.info("Created Pulsar producer");
        // Send few test messages
        double total_time = 0;
        List<Long> times = new ArrayList();
        for (int i = 0; i < 1001; i++) {
            String content = String.format("hello!!!!!!-wanna!-%d", i);
            long startTime = System.currentTimeMillis();
            // Build a message object
            // Message msg = MessageBuilder.create().setContent(content.getBytes()).build();

            // Send a message (waits until the message is persisted)
            // MessageId msgId = producer.newMessage()
            // .value(content.getBytes())
            // .replicationClusters(restrictReplicationTo)
            // .send();

            // async send
            log.info("{} before send{}", i, System.currentTimeMillis());
            times.add(System.currentTimeMillis());
            producer.newMessage()
                  .value(content.getBytes())
                  .sendAsync().thenAccept(messageId -> {
                log.info("Published message {} at {}", messageId, System.currentTimeMillis());
            }).exceptionally(e -> {
                System.out.println("Failed to publish " + e);
                return null;
            });
            log.info("{} after send{}", i, System.currentTimeMillis());
            // total_time += (System.currentTimeMillis() - startTime*1.0)/1000;
        //     // MessageId msgId = producer.send(msg);

            // log.info("Published msg='{}' with msg-id={}", content, msgId);
        }
        // log.info("total time is {} s", total_time);
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            //TODO: handle exception
        }
        File writeFile = new File("./out.csv");
 
        try{
            BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));
            for(int i=0;i<times.size();i++){
                writeText.newLine();    
                writeText.write(String.valueOf(times.get(i)));
            }

            writeText.flush();
            writeText.close();
        }catch (FileNotFoundException e){
            System.out.println("没有找到指定文件");
        }catch (IOException e){
            System.out.println("文件读写出错");
        }
        client.close();
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerTutorial.class);
}
