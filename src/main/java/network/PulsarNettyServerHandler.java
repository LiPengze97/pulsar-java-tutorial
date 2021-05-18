package network;

import java.io.UnsupportedEncodingException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.List;
import java.util.ArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;import java.util.Arrays;

import myutil.Request;
/**
 * 处理客户端的请求
 */
public class PulsarNettyServerHandler extends ChannelInboundHandlerAdapter {
    private static ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    public long[] time_recorder;
    int site_id = -1;
    int throughput = -1;
    int msg_num;

    public PulsarNettyServerHandler(){}

    public PulsarNettyServerHandler(int msg_num, int throughput){
        initialize_time_recorder(msg_num);
        this.throughput = throughput;
    }

    public void initialize_time_recorder(int msg_num){
        this.msg_num = msg_num;
        time_recorder = new long[msg_num+1];
    }

    public void record_time(int msg_id){
        time_recorder[msg_id] = System.currentTimeMillis();
    }

    public void output_data(){
        String file_name = "./"+String.valueOf(throughput) + "_site_" + String.valueOf(site_id)+".csv";
        System.out.println(site_id+" is outputing!");
        File writeFile = new File(file_name);
 
        try{
            BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));
            writeText.write(String.valueOf(site_id));
            writeText.newLine();
            for(int i = 0; i < msg_num; i++){
                writeText.write(String.valueOf(time_recorder[i]));
                writeText.newLine();
            }
            writeText.flush();
            writeText.close();
        }catch (FileNotFoundException e){
            System.out.println("没有找到指定文件");
        }catch (IOException e){
            System.out.println("文件读写出错");
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        //通知其它客户端
        System.out.println("【服务端：】" + channel.remoteAddress() + "加入");
        // channelGroup.writeAndFlush(\n");
        channelGroup.add(channel);
    }

    // 读取数据
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {       
        handlerObject(ctx, msg);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        //通知其它客户端
        System.out.println("新handler加入");
        // channelGroup.writeAndFlush("【服务端：】" + channel.remoteAddress() + "加入\n");
        // channelGroup.add(channel);
    }
    
    /**
     * 最简单的处理
     * @param ctx
     * @param msg
     * @throws UnsupportedEncodingException
     */
    public void simpleRead(ChannelHandlerContext ctx, Object msg) throws UnsupportedEncodingException{

        ByteBuf bb = (ByteBuf)msg;
        // 创建一个和buf同等长度的字节数组
        byte[] reqByte = new byte[bb.readableBytes()];
        // 将buf中的数据读取到数组中
        bb.readBytes(reqByte);
        String reqStr = new String(reqByte, "utf-8");
        System.err.println("server 接收到客户端的请求： " + reqStr);
        String respStr = new StringBuilder("来自服务器的响应").append(reqStr).append("$_").toString();
        
        // 返回给客户端响应和客户端链接中断即短连接，当信息返回给客户端后中断
        ctx.writeAndFlush(Unpooled.copiedBuffer(respStr.getBytes()));//.addListener(ChannelFutureListener.CLOSE);
        // 有了写操作（writeAndFlush）下面就不用释放msg
        ReferenceCountUtil.release(msg);
    }
    
    // 数据读取完毕的处理
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // System.err.println("服务端读取数据完毕");
    }
    
    // 出现异常的处理
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.println("server 读取数据出现异常");
        ctx.close();
    }
    
    /**
     * 将请求信息直接转成对象
     * @param ctx
     * @param msg
     */
    private void handlerObject(ChannelHandlerContext ctx, Object msg) {
        Request req = (Request)msg;
        if(site_id == -1){
            site_id = req.get_site_id();
        }
        // System.out.println("message_id: "+req.get_message_id()+"site_id: "+req.get_site_id());
        if(Integer.valueOf(req.get_message_id()) == (-1)){
            output_data();
        }
        record_time(req.get_message_id());
        ReferenceCountUtil.release(msg);
    }
    
    
}
