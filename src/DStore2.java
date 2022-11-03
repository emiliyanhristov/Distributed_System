import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class DStore2 {

    static String currentFileName;
    static int currentFileSize;
    static int cport;
    static String file_folder;
    static String fileToSend;

    public static void main(String[] args) {
        /*
        final int port = Integer.parseInt(args[0]);
        final int cport = Integer.parseInt(args[1]);
        final int timeout = Integer.parseInt(args[2]);
        final String file_folder = args[3];
         */

        final int port = 10002;
        final int cport = 12345;
        setCport(cport);
        final int timeout = 5000;
        final String file_folder = "DStore2";
        setFile_folder(file_folder);

        emptyContent(file_folder);

        try {
            ServerSocket ss = new ServerSocket(port); //port to listenTo
            System.out.println("DStore is listening on port " + port);
            try {
                Socket controller = new Socket(InetAddress.getLoopbackAddress(), cport);
                PrintWriter controllerPrintWriter = new PrintWriter(controller.getOutputStream(), true);
                controllerPrintWriter.println("JOIN " + port);
                while(true) {
                    System.out.println("Joined!");
                    System.out.println("Waiting for connection!");
                    Socket client = ss.accept();
                    System.out.println("Connected!");
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                OutputStream clientOut = client.getOutputStream();
                                PrintWriter printWriter = new PrintWriter(clientOut, true);
                                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                String line;
                                while ((line = in.readLine()) != null) {
                                    String[] contents = line.split(" ");
                                    if (contents[0].equals("STORE")) {
                                        receiveStore(printWriter, contents[1], Integer.parseInt(contents[2]));
                                        client.setSoTimeout(timeout);
                                        receiveFileContent(controllerPrintWriter, client.getInputStream(), currentFileSize, true);
                                    } else if (contents[0].equals("LOAD_DATA")){
                                        load(contents[1], clientOut);
                                    } else if (contents[0].equals("REMOVE")){
                                        removeFile(controllerPrintWriter, contents[1]);
                                    } else if (contents[0].equals("LIST")){
                                        System.out.println("List received");
                                        listFiles(controllerPrintWriter);
                                    } else if (contents[0].equals("REBALANCE")){
                                        String remainingRebalance = line.split(" ", 2)[1];
                                        System.out.println(remainingRebalance);
                                        handleRebalance(controllerPrintWriter, remainingRebalance);
                                    } else if (contents[0].equals("REBALANCE_STORE")){
                                        receiveStore(printWriter, contents[1], Integer.parseInt(contents[2]));
                                        receiveFileContent(printWriter, client.getInputStream(), currentFileSize, false);
                                    } else if (contents[0].equals("ACK")){
                                        System.out.println("ACK received");
                                        sendFileContent(clientOut);
                                    }
                                }
                                client.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();
                }
            } catch(Exception e) { System.err.println("error: " + e); }
        } catch(Exception e) { System.err.println("error: " + e);}
    }

    private static void emptyContent(String file_folder) {
        File directory = new File(file_folder);
        if(!directory.exists()){
            directory.mkdir();
        }
        if(!isEmpty(file_folder)){
            clearDirectory(directory);
            System.out.println("Directory cleared!");
        }
    }

    private static void clearDirectory(File dir){
        for(File file : dir.listFiles()){
            file.delete();
        }
    }

    private synchronized static void receiveStore(PrintWriter printWriter, String fileName, int fileSize) {
        try {
            currentFileName = fileName;
            currentFileSize = fileSize;
            printWriter.println("ACK");
            System.out.println("ACK sent");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized static void receiveFileContent(PrintWriter printWriter, InputStream inputStream, int fileSize, boolean storeAckFlag) {
        try {
            File file = new File(file_folder + File.separator + currentFileName);
            byte[] data = new byte[fileSize];
            inputStream.readNBytes(data, 0, fileSize);

            FileOutputStream fos = new FileOutputStream(file);
            fos.write(data);
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(storeAckFlag) {
            printWriter.println("STORE_ACK " + currentFileName);
        }
        System.out.println("File stored");
    }

    private synchronized static void load(String fileName, OutputStream out){
        try {
            System.out.println("Loading file " + fileName);
            File file = new File(file_folder + File.separator + fileName);
            byte[] data = Files.readAllBytes(file.toPath());
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private synchronized static void removeFile(PrintWriter printWriter, String fileName){
        File file = new File(file_folder + File.separator + fileName);
        if(file.exists()) {
            if(file.delete()) {
                printWriter.println("REMOVE_ACK " + fileName);
                System.out.println("Removed");
            } else {
                System.out.println("Not Removed!");
            }
        } else {
            printWriter.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
            System.out.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
        }
    }

    private synchronized static void listFiles(PrintWriter printWriter){
        String files = "";
        File folder = new File(file_folder);
        for(String file : folder.list()){
            files = files + " " + file;
        }
        printWriter.println("LIST" + files);
        System.out.println("List sent to controller" + files);
    }

    private static boolean isEmpty(String path) {
        try (Stream<Path> entries = Files.list(Path.of(path))) {
            return !entries.findFirst().isPresent();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private synchronized static void handleRebalance(PrintWriter controllerPW, String rebalanceInfo){
        String[] contents = rebalanceInfo.split(" ");
        int currrentIndex = 0;
        int files_to_send_counter = 0;
        int send_index = 1;
        while(files_to_send_counter < Integer.valueOf(contents[0])){
            String fileName = contents[send_index];
            int number_of_ports = Integer.valueOf(contents[send_index+1]);
            for(int i = 1; i <= number_of_ports; i++){
                int port = Integer.valueOf(contents[send_index+1+i]);
                sendFile(port, fileName);
            }
            files_to_send_counter++;
            send_index = send_index + number_of_ports + 2;
        }
        int remove_index = send_index;
        int files_to_remove_counter = Integer.valueOf(contents[remove_index]);
        for(int i = 1; i <= files_to_remove_counter; i++){
            removeFileRebalance(contents[remove_index+i]);
        }
        controllerPW.println("REBALANCE_COMPLETE");
        System.out.println("REBALANCE_COMPLETE");
    }

    private synchronized static void sendFile(Integer port, String fileName){
        try {
            long fileSize = Files.size(Path.of(file_folder + File.separator + fileName));
            Socket dStore = new Socket(InetAddress.getLoopbackAddress(), port);
            OutputStream out = dStore.getOutputStream();
            PrintWriter dStorePW = new PrintWriter(out, true);
            dStorePW.println("REBALANCE_STORE " + fileName + " " + fileSize);
            System.out.println("REBALANCE_STORE " + fileName + " " + fileSize);
            fileToSend = fileName;
            sendFileContent(out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized static void sendFileContent(OutputStream out){
        System.out.println("Sending contents!");
        File file = new File(file_folder + File.separator + fileToSend);
        try {
            byte[] data = Files.readAllBytes(file.toPath());
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized static void removeFileRebalance(String fileName){
        System.out.println(fileName);
        File file = new File(file_folder + File.separator + fileName);
        if(file.exists()) {
            if(file.delete()) {
                System.out.println("Removed");
            } else {
                System.out.println("Not removed");
            }
        }
    }
    //Getters and Setters ------------------------------------------------------------------------------------------------------------

    public static int getCport() {
        return cport;
    }

    public static void setCport(int cport) {
        DStore2.cport = cport;
    }

    public static String getFile_folder() {
        return file_folder;
    }

    public static void setFile_folder(String file_folder) {
        DStore2.file_folder = file_folder;
    }
}