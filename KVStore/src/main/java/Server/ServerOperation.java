package Server;

import com.keyval_grpc.internalFiles.KeyValueGrpc;
import com.keyval_grpc.internalFiles.Keyval;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

public class ServerOperation extends KeyValueGrpc.KeyValueImplBase {
    String input;
    public String filepath;
    public String outputDirectory;
    public static ConcurrentHashMap<String, String> keyToValue;
    public static ConcurrentHashMap<String, String> keyToBytes;
    public static Set<String> keySet;
    String result;
    public ServerOperation(){
        input = "";
        filepath ="";
        keyToValue = new ConcurrentHashMap();
        keyToBytes = new ConcurrentHashMap();
        keySet = Collections.synchronizedSet(new HashSet());
        result = "";
    }



    @Override
    public void serverOperation(Keyval.Request request, StreamObserver<Keyval.Response> responseObserver) {
        //super.serverOperation(request, responseObserver);
        input = request.getInput();
        filepath = request.getFilepath();
        outputDirectory=request.getMainDirectory();
        System.out.println("server opes  "+filepath);
        Path path = Paths.get(filepath);
        if(!path.toFile().exists()) {
            path.toFile().mkdirs();
            System.out.println("Created path "+path);
        }
        path.toFile().setWritable(true);
        path.toFile().setReadable(true);
        path.toFile().setExecutable(true);
        try {
            ExecutorService executor = Executors.newCachedThreadPool();
            ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
            pool.setCorePoolSize(70);
            Future<String> futureCall = pool.submit(new ServerThread(input,filepath));
            System.out.println("Connected");
            try {
                    while(!futureCall.isDone()) {
                        Thread.sleep(10);
                    }
                    result = futureCall.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        Keyval.Response.Builder response  = Keyval.Response.newBuilder();
        response.setOutput(result);
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();

    }
}

