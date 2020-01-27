package Server;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.Scanner;
import java.util.concurrent.Callable;



public class ServerThread extends ServerOperation implements Callable {
    private static Socket clientSocket;
    private static String outputValue;
    private static String outputBytes;
    private static String finalOutput;
    private static String setValue;
    private ServerOperation mainObject;
    private ServerThread serverRunnableObject;
    private static InputStream inputStream;
    private static InputStreamReader inputStreamReader;
    private static BufferedReader bufferedReader;
    private String input;
    private static Path PATH;

    ServerThread() throws RemoteException {
        super();
    }

    ServerThread(String input,String outputDirectory) throws RemoteException {
        super();
        outputValue = "";
        outputBytes = "";
        setValue = "";
        finalOutput = "";
        this.input = input;
        this.filepath = filepath;
        mainObject = new ServerOperation();
        String tmpdirpath = System.getProperty("java.io.tmpdir");
        PATH = Paths.get(outputDirectory);
        System.out.println("path "+PATH);
    }

    @Override
    public String call() {
        System.out.println("New Connection Established");

        String[] parsedInput = input.split("\\s+");
        String key;
        int bytes;
        String value = "";
        System.out.println("key is" + parsedInput[0]);
        if (parsedInput[0].equals("GET")) {
            System.out.println("COMMAND: GET");
            key = parsedInput[1];
            if (mainObject.keySet.contains(key)) {
                finalOutput = "GET UNSUCCESSFUL\n";
            } else
                parseGetRequest(parsedInput[1]);

        } else if (parsedInput[0].equals("SET")) {
            System.out.println("COMMAND: SET");

            key = parsedInput[1];
            if (mainObject.keySet.contains(key)) {
                finalOutput = "UNSUCCESSFUL\n";
            } else {
                mainObject.keySet.add(key);
                bytes = Integer.parseInt(parsedInput[2]);
                String temp = parsedInput[3];
                parseSetRequest(key, bytes, temp);
                mainObject.keySet.remove(key);
            }

        }
        else if(parsedInput[0].equals("UPDATE")){
            key = parsedInput[1];

        }
        return finalOutput;
    }

    private String returnOutput() {
        return finalOutput;
    }

    private void parseGetRequest(String key) {
        System.out.println("<key>: " + key);
        try {
            serverRunnableObject = new ServerThread();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        serverRunnableObject.get(key);
        if (outputValue.length() > 0) {
            finalOutput += "VALUE ";
            finalOutput += key + " " + outputBytes + "\r\n";
            finalOutput += outputValue + "\r\n" + "END\r\n";
            System.out.println("<value-in-bytes>: " + outputBytes);
            System.out.println("<value>: " + outputValue);

        } else {
            finalOutput += "UNSUCCESSFUL";
        }
        System.out.println("<value>: " + finalOutput);

    }

    private void parseSetRequest(String key, int bytes, String completeInput) {
        System.out.println("<key>: " + key);
        System.out.println("<value>: " + completeInput);
        try {
            serverRunnableObject = new ServerThread();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        byte[] b = completeInput.getBytes();
        //setValue = completeInput;
        int i = 0;
        while (setValue.getBytes().length != bytes) {
            setValue += (char) b[i];
            i++;
        }

        Boolean result = serverRunnableObject.set(key, setValue);
        if (result) {
            finalOutput = "SUCCESS\n";
            System.out.println("Updating Local cache with current key and value.");
            mainObject.keyToValue.put(key, setValue);
            mainObject.keyToBytes.put(key, Integer.toString(setValue.getBytes().length));
        } else {
            System.out.println("Error while Setting <key-value>: " + key);
            finalOutput = "UNSUCCESSFUL\n";
        }
    }

    private void get(String key) {
        if (mainObject.keyToValue.containsKey(key)) {
            System.out.println("Fetching value from LOCAL CACHE");
            outputValue = keyToValue.get(key);
            outputBytes = keyToBytes.get(key);
        } else {
            System.out.println("Fetching value from FILE corresponding to respective key");
            String filename = "/"+key+".txt";
            System.out.println("path with file name "+PATH + filename);
            File file = new File(PATH + filename);

            if (Files.exists(PATH)) {
                if (file.exists()) {
                    try {
                        Scanner sc = new Scanner(file);
                        while (sc.hasNextLine()) {
                            outputValue += sc.nextLine();
                        }
                        outputBytes += outputValue.getBytes().length;

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        System.out.println("Updating current key and value in local cache");
                        mainObject.keyToValue.put(key, outputValue);
                        mainObject.keyToBytes.put(key, outputBytes);
                    }
                }
            }
        }
    }

    private Boolean set(String key, String value) {
        //System.out.println("file path in serverthread "+this.filepath);
        try {
            String filename = "/"+key+".txt";
            File file = new File(PATH+ filename);
            Path path = Paths.get("./" + this.filepath);

            if (Files.exists(PATH)) {
                if (!file.exists()) {
                    file.createNewFile();
                } else {
                    file.delete();
                    file.createNewFile();
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter(file.getPath()));
                writer.write(value);
                writer.close();
            }
//            else{
//                File f = new File("./"+this.filepath);
//                f.mkdirs();
//                File f1 = new File("./" +this.filepath+ "/" + key + ".txt");
//                f1.createNewFile();
//                BufferedWriter writer = new BufferedWriter(new FileWriter(file.getPath()));
//                writer.write(value);
//                writer.close();
//            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

}
