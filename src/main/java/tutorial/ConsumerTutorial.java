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

        ChannelFuture cf = new ClientNetty(ip, port).action();

        PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
        List<Long> times = new ArrayList();

        Consumer<byte[]> consumer = client.newConsumer()
            .topic("non-persistent://my-tenant/my-namespace/wana")
            .subscriptionName("my-subscription")
            .replicateSubscriptionState(true)
            .subscribe();
        log.info("Created Pulsar consumer");
        int cnt= 0;
        while (true) {
            // Wait until a message is available
            Message msg = consumer.receive();

            // Do something with the message
            // String content a= new String(msg.getData());
            String content = byteArrayToStr(msg.getData());
            log.info("Receiveed message with {} Byte with msg-id={} from{}", content.length(), msg.getMessageId(), msg.getReplicatedFrom());
	        // long timee = System.currentTimeMillis() - msg.getPublishTime();
            // times.add(timee);
	        // log.info("used {} ms since {}", timee, msg.getPublishTime());
            // Acknowledge processing of message so that it can be deleted
            consumer.acknowledge(msg);
            // to netty server
            Request req = new Request(cnt, local_id);
            cf.channel().writeAndFlush(req);
            if(++cnt==msg_num){break;}
        }
        log.info("send finished");
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
