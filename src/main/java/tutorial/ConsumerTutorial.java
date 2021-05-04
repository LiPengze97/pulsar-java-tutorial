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

import io.netty.channel.ChannelFuture;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClientException;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import network.ClientNetty;

import myutil.FileUtil;
import myutil.Request;

public class ConsumerTutorial {

    private static final String SERVICE_URL = "pulsar://localhost:6650";

    private static final String TOPIC_NAME = "persistent://sample/standalone/ns1/tutorial-topic";

    private static final String SUBSCRIPTION_NAME = "tutorial-subscription";

    public static String byteArrayToStr(byte[] byteArray) {
        if (byteArray == null) {
            return null;
        }
        String str = new String(byteArray);
        return str;
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        
        String filePath = "pulsar.json";
        String jsonContent = FileUtil.ReadFile(filePath);
        JSONObject jsonobject = JSON.parseObject(jsonContent);
        int port = jsonobject.getIntValue("port");
        String ip = jsonobject.getString("ip");
        int local_id = jsonobject.getIntValue("local_site_id");
        int msg_num = jsonobject.getIntValue("message_num");
        int msg_size = jsonobject.getIntValue("message_size");

        ChannelFuture cf = new ClientNetty(ip, port).action();

        PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
        List<Long> receive_times = new ArrayList();
        List<Long> start_times = new ArrayList();
        Consumer<byte[]> consumer = client.newConsumer()
            .topic("non-persistent://my-tenant/my-namespace/wana")
            .subscriptionName("my-subscription")
            .replicateSubscriptionState(true)
            .receiverQueueSize(10000)
            .subscribe();
        log.info("Created Pulsar consumer");
        int cnt= 0;
        double total_byte = 0;
        long all_start_time = 0, last_message_time = 0;
        ExecutorService es = Executors.newFixedThreadPool(1);
        
        while (true) {
            // Wait until a message is available
            // Message msg = consumer.receive();
                es.execute(new Runnable() {
                @Override
                public void run(){
                    try {
                        while(true){
                            Message msg = consumer.receive();
                            receive_times.add(System.currentTimeMillis());
                            // last_message_time = System.currentTimeMillis();
                            if(start_times.size() == 0){
                                start_times.add(System.currentTimeMillis());
                            }
                            // Do something with the message
                            // String content a= new String(msg.getData());
                            String content = byteArrayToStr(msg.getData());
                            // log.info("Receiveed message with {} Byte with msg-id={} from{}", content.length(), msg.getMessageId(), msg.getReplicatedFrom());
                            System.out.println("cnt " + receive_times.size() + " , message key" + msg.getKey());
                            // long timee = System.currentTimeMillis() - msg.getPublishTime();
                            // times.add(timee);
                            // log.info("used {} ms since {}", timee, msg.getPublishTime());
                            // Acknowledge processing of message so that it can be deleted
                            consumer.acknowledge(msg);
                            // to netty server
                            Request req = new Request(receive_times.size(), local_id);
                            cf.channel().writeAndFlush(req);
                            // total_byte += content.length();
                        }
                        
                    } catch (PulsarClientException e) {
                        // System.out.println("interrupt, then quit");
                        // return;
                    }
                }
            });
            es.shutdown();
            boolean finished = es.awaitTermination(10, TimeUnit.SECONDS);
            if (!finished) {
                es.shutdownNow();
                break;
            }
            // last_message_time = System.currentTimeMillis();
            // if(all_start_time == 0){
            //     all_start_time = System.currentTimeMillis();
            // }
            // // Do something with the message
            // String content = byteArrayToStr(msg.getData());
            // System.out.println("cnt " + cnt + " , message key" + msg.getKey());
            // // Acknowledge processing of message so that it can be deleted
            // consumer.acknowledge(msg);
            // // to netty server
            // Request req = new Request(cnt, local_id);
            // cf.channel().writeAndFlush(req);
            // total_byte += content.length();
            // if(++cnt==msg_num){break;}
        }
        // double total_time = (System.currentTimeMillis() - all_start_time*1.0) / 1000.0;
        // double total_time = (last_message_time - all_start_time*1.0) / 1000.0;
        double total_time = (receive_times.get(receive_times.size()-1) - start_times.get(0)*1.0) / 1000.0;
        // total_byte /= 1024.0*1024.0;
        total_byte = msg_size * receive_times.size() * 1.0 / (1024.0*1024.0);
        log.info("{} msg/s, {} MByte/s", receive_times.size()/total_time, total_byte/total_time);
        Request req = new Request(-1, local_id);
        cf.channel().writeAndFlush(req);
        client.close();
        // File writeFile = new File("./out.csv");
        // try{

        //     BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));

        //     for(int i=0;i<times.size();i++){
        //         writeText.write(String.valueOf(times.get(i)));
        //         writeText.newLine();
        //     }
        //     writeText.flush();
        //     writeText.close();
        // }catch (FileNotFoundException e){
        //     System.out.println("没有找到指定文件");
        // }catch (IOException e){
        //     System.out.println("文件读写出错");
        // }
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerTutorial.class);

}
