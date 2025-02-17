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
public class ServerHandler extends ChannelInboundHandlerAdapter {
    public long[] time_recorder;
    int server_num;
    int msg_num;
    public void initialize_time_recorder(int msg_num, int server_num){
        this.msg_num = msg_num;
        this.server_num = server_num;
        time_recorder = new long[msg_num*(server_num+2)];
    }

    public void record_time(int msg_id, int site_id){
        time_recorder[msg_id*(server_num+1)+site_id] = System.currentTimeMillis();
    }

    public void output_data(){
        File writeFile = new File("./out.csv");
 
        try{
            BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));
            writeText.write("start,");
            for(int i = 0; i < server_num; i++){
                if(i != server_num - 1){
                    writeText.write(String.valueOf(i+1)+",");
                }else{
                    writeText.write(String.valueOf(i+1));
                    writeText.newLine();
                }
            }
            
            for(int i = 0; i < msg_num; i++){
                for(int j = 0; j <= server_num; j++){
                    // String content = String.valueOf(time_recorder[msg_num * (server_num+1) + j]);
                    // String content1 = String.valueOf(time_recorder[i]);
                    // String content2 = String.valueOf(i * (server_num+1) + j);
                    // System.out.println(content);
                    // System.out.println(content1);
                    // System.out.println(content2);
                    if(j != server_num){
                        writeText.write(String.valueOf(time_recorder[i * (server_num+1) + j])+",");
                    }else{
                        writeText.write(String.valueOf(time_recorder[i * (server_num+1) + j]));
                        writeText.newLine();
                    }
                }
            }
            writeText.flush();
            writeText.close();
        }catch (FileNotFoundException e){
            System.out.println("没有找到指定文件");
        }catch (IOException e){
            System.out.println("文件读写出错");
        }
    }

    // 读取数据
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {       
        // 普通的处理 及过滤器不多
        if (msg instanceof Request){
            // System.out.println("Request");
            handlerObject(ctx, msg);
        }else{
            // System.out.println("string");
            simpleRead(ctx, msg);  
        }
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
//      ReferenceCountUtil.release(msg);
    }
    
    /**
     * 有分隔符的请求信息分包情况处理，包含了转码
     * @param ctx
     * @param msg
     */
    private void Delimiterread(ChannelHandlerContext ctx, Object msg) {
        // 如果把msg直接转成字符串，必须在服务中心添加 socketChannel.pipeline().addLast(new StringDecoder());
        String reqStr = (String)msg;
        System.err.println("server 接收到请求信息是："+reqStr);
        String respStr = new StringBuilder("来自服务器的响应").append(reqStr).append("$_").toString();
        
        // 返回给客户端响应和客户端链接中断即短连接，当信息返回给客户端后中断
        ctx.writeAndFlush(Unpooled.copiedBuffer(respStr.getBytes())).addListener(ChannelFutureListener.CLOSE);
    }

    
    // 数据读取完毕的处理
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        System.err.println("服务端读取数据完毕");
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
        // 需要序列化 直接把msg转成对象信息，一般不会用，可以用json字符串在不同语言中传递信息
        Request req = (Request)msg;
        System.err.println("message_id: "+req.get_message_id()+"site_id: "+req.get_site_id());
        record_time(req.get_message_id(),req.get_site_id());
        // ctx.writeAndFlush(req);
    }
    
    
}
