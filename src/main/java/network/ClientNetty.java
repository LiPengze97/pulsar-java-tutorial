package network;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import java.io.UnsupportedEncodingException;

import java.util.Random;
import java.util.Scanner;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import myutil.MyConstant;
import myutil.FileUtil;
import myutil.Request;
/**
 * 客户端发送请求
 *
 */
public class ClientNetty {
    
    // 要请求的服务器的ip地址
    private String ip;
    // 服务器的端口
    private int port;
    
    public ClientNetty(String ip, int port){
        this.ip = ip;
        this.port = port;
    }
    
    // 请求端主题
    public ChannelFuture action() throws InterruptedException, UnsupportedEncodingException {
        
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        
        Bootstrap bs = new Bootstrap();
        
        bs.group(bossGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel socketChannel) throws Exception {              
                    // 处理来自服务端的响应信息
                    socketChannel.pipeline().addLast(new ObjectEncoder(),
                                                     new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                                     new ClientHandler());
              }
         });
        
        // 客户端开启
        ChannelFuture cf = bs.connect(ip, port).sync();
        return cf;
        // for(int i = 0; i < 100; i++){
        //     Request req = new Request(i,i+1000);
        //     cf.channel().writeAndFlush(req);
        // }
        
        // 等待直到连接中断
        // cf.channel().closeFuture().sync();      
    }
            
    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {
        String filePath = "pulsar.json";
        String jsonContent = FileUtil.ReadFile(filePath);
        JSONObject jsonobject = JSON.parseObject(jsonContent);
        int port = jsonobject.getIntValue("port");
        String ip = jsonobject.getString("ip");
        Random rand = new Random();
        int local_id = rand.nextInt(10);
        ChannelFuture cf = new ClientNetty(ip, port).action();
        Scanner scan = new Scanner(System.in);
        System.out.println("Netty Client ready, press enter to send message");
        scan.nextLine();
        for(int i = 0; i < 100; i++){
            // cf.channel().writeAndFlush(new Request(i, local_id));
            cf.channel().writeAndFlush("???");
        }
        cf.channel().closeFuture().sync();  
    }
        
}
