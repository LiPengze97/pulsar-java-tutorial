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
    public static void main(String[] args) throws IOException {
        // Create a Pulsar client instance. A single instance can be shared across many
        // producers and consumer within the same application
        PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
        List<Long> times = new ArrayList();
        // Here you get the chance to configure consumer specific settings. eg:
        // ConsumerConfiguration conf = new ConsumerConfiguration();

        // Allow multiple consum;ers to attache to the same subscription
        // and get messages dispatched as a Queue
        // conf.setSubscriptionkType(SubscriptionType.Shared);

        // Once the consumer is created, it can be used for the entire application life-cycle
        // Consumer consumer = client.subscribe(TOPIC_NAME, SUBSCRIPTION_NAME, conf);
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
            log.info("Receiveed message '{}' with msg-id={} from{}", content, msg.getMessageId(), msg.getReplicatedFrom());
	        long timee = System.currentTimeMillis() - msg.getPublishTime();
            times.add(timee);
	        log.info("used {} ms since {}", timee, msg.getPublishTime());
            // Acknowledge processing of message so that it can be deleted
            consumer.acknowledge(msg);
           if(cnt++==1000){break;}
        }
        File writeFile = new File("./out.csv");
        try{
            //第二步：通过BufferedReader类创建一个使用默认大小输出缓冲区的缓冲字符输出流
            BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));
 
            //第三步：将文档的下一行数据赋值给lineData，并判断是否为空，若不为空则输出
            for(int i=0;i<times.size();i++){
                writeText.newLine();    //换行
                //调用write的方法将字符串写到流中
                writeText.write(String.valueOf(times.get(i)));
            }
 
            //使用缓冲区的刷新方法将数据刷到目的地中
            writeText.flush();
            //关闭缓冲区，缓冲区没有调用系统底层资源，真正调用底层资源的是FileWriter对象，缓冲区仅仅是一个提高效率的作用
            //因此，此处的close()方法关闭的是被缓存的流对象
            writeText.close();
        }catch (FileNotFoundException e){
            System.out.println("没有找到指定文件");
        }catch (IOException e){
            System.out.println("文件读写出错");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerTutorial.class);

}
