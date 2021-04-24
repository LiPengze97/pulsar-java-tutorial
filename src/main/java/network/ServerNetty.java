package network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.Scanner;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import myutil.MyConstant;
import myutil.FileUtil;
import myutil.Request;

/**
 * tcp/ip 服务端用netty实现
 *
 */
public class ServerNetty {
    
    private int port;   
    public ServerNetty(int port){
        this.port = port;
    }
    private ServerHandler serverhandler;
    
    // netty 服务端启动
    public void action() throws InterruptedException{
        
        // 用来接收进来的连接
        EventLoopGroup bossGroup = new NioEventLoopGroup(); 
        // 用来处理已经被接收的连接，一旦bossGroup接收到连接，就会把连接信息注册到workerGroup上
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        
        try {
            // nio服务的启动类
            ServerBootstrap sbs = new ServerBootstrap();
            serverhandler = new ServerHandler();
            // 配置nio服务参数
            sbs.group(bossGroup, workerGroup)
               .channel(NioServerSocketChannel.class) // 说明一个新的Channel如何接收进来的连接
               .option(ChannelOption.SO_BACKLOG, 128) // tcp最大缓存链接个数
               .childOption(ChannelOption.SO_KEEPALIVE, true) //保持连接
               .handler(new LoggingHandler(LogLevel.INFO)) // 打印日志级别
               .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        // 处理接收到的请求
                        socketChannel.pipeline().addLast(new ObjectEncoder(),
                                                         new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                                         serverhandler); // 这里相当于过滤器，可以配置多个
                    }
               });
            // 绑定端口，开始接受链接
            ChannelFuture cf = sbs.bind(port).sync();
            Scanner scan = new Scanner(System.in);
            System.out.println("Netty Server ready, press enter to send message");
            scan.nextLine();
            // 等待服务端口的关闭；在这个例子中不会发生，但你可以优雅实现；关闭你的服务
            // cf.channel().closeFuture().sync();
        } finally{
            // bossGroup.shutdownGracefully();
            // workerGroup.shutdownGracefully();
        }           
    }
    
    public void record_time(int msg_id, int site_id){
        serverhandler.record_time(msg_id, site_id);
    }

    public void initialize_time_recorder(int msg_num, int server_num){
        serverhandler.initialize_time_recorder(msg_num, server_num);
    }

    public void output_data(){
        serverhandler.output_data();
    }
        
    // 开启netty服务线程
    public static void main(String[] args) throws InterruptedException {
        String filePath = "pulsar.json";
        String jsonContent = FileUtil.ReadFile(filePath);
        JSONObject jsonobject = JSON.parseObject(jsonContent);
        int port = jsonobject.getIntValue("port");

        new ServerNetty(port).action();
        
    }
}
