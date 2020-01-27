package MainServer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import Server.ServerOperation;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MainServer {
    public static void main(String[] args) throws IOException {
        //InetSocketAddress hostaddress=  new InetSocketAddress("128.89.23.1");
        //NettyServerBuilder server = NettyServerBuilder.forAddress("21.56.90.10").forPort(1993).addService(new ServerOperation()).build();
        Server server = ServerBuilder.forPort(1993).addService(new ServerOperation()).build();
        server.start();
        System.out.println("Server started at port: "+server.getPort());
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
