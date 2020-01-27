package MapReduce;

import com.keyval_grpc.internalFiles.KeyValueGrpc;
import com.keyval_grpc.internalFiles.Keyval;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;

public class WordCountMapper {
    private static ArrayList<String> words;
    public WordCountMapper(ArrayList<String> words){
        this.words = words;
    }
    public static void updateKeyVal(KeyValueGrpc.KeyValueBlockingStub stub) {
        String responsemsg = "";
        String newresponse = "";
        HashSet<String> set = new HashSet();
        int count = 5000;
        int i=0;
        for (String s : words) {
            i++;
            if(i == count) break;
            int len = s.length();
            if(len > 1)
            {
                if (set.contains(s)) {
                    System.out.println(s+" contained");
                    String requestmsg = "GET " + s + "\n";
                    String updatedRequestValue = "";
                    Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath("Map2").build();
                    responsemsg = stub.serverOperation(request).getOutput();
                    String[] msgs = responsemsg.split("\n");
//                System.out.println("response msg " + responsemsg + " for request " + s);
                    if (!responsemsg.trim().equals("UNSUCCESSFUL")) {
                        responsemsg = msgs[1].trim();
                        System.out.println("response msg " + responsemsg );
                        int newval = Integer.parseInt(responsemsg) + 1;
                        updatedRequestValue += newval;
                        System.out.println("updated val "+updatedRequestValue);
                        int noOfBytes = updatedRequestValue.getBytes().length;
                        String num = Integer.toString(noOfBytes);
                        requestmsg = "SET " + s + " " + num + "\r\n" + updatedRequestValue + "\r\n";
                        request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath("Map2").build();
                        newresponse = stub.serverOperation(request).getOutput();
                        System.out.println("response "+newresponse);
                    }
                } else {
                    System.out.println("new s " +s);
                    set.add(s);
                    String requestValue = "1";
                    int noOfBytes = requestValue.getBytes().length;
                    String num = Integer.toString(noOfBytes);
                    String requestmsg = "SET " + s + " " + num + "\r\n" + requestValue + "\r\n";
                    Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath("Map2").build();
                    String newResponse = stub.serverOperation(request).getOutput();
                }
            }
        }
    }

    public static void main(String[] args){
        if(args.length != 1) {
            System.err.println("Invalid command line, exactly one argument required");
            System.exit(1);

        }
        words = new ArrayList<>();
        try {
            FileInputStream fstream = new FileInputStream(args[0]);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fstream));
            String word;
            while ((word = reader.readLine()) != null) {
                words.add(word);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost",1993).usePlaintext().build();
//        KeyValueGrpc.KeyValueBlockingStub stub = KeyValueGrpc.newBlockingStub(channel);
//        updateKeyVal(stub);
    }


}
