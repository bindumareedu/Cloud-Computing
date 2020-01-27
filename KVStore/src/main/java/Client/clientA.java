package Client;

import Server.ServerOperation;
import com.keyval_grpc.internalFiles.KeyValueGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.keyval_grpc.internalFiles.Keyval.Response;
import com.keyval_grpc.internalFiles.Keyval.Request;


public class clientA {
    public static void main(String[] args){
        ManagedChannel channel = ManagedChannelBuilder.forAddress("215.0.196.35.bc.googleusercontent.com",1993).usePlaintext().build();
        KeyValueGrpc.KeyValueBlockingStub stub = KeyValueGrpc.newBlockingStub(channel);
        String value = "testing";
        String bytes = Integer.toString(value.getBytes().length);
       String requestval = "SET key8 "+bytes+"\r\n"+value+"\r\n";
      //  String requestval = "GET key8\n";
        Request request = Request.newBuilder().setInput(requestval).setFilepath(".").build();

        Response response = stub.serverOperation(request);
        System.out.println("This is output "+response.getOutput());
    }
}
