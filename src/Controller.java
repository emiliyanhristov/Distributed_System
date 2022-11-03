import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

class Controller {

    static Index index = new Index(new ArrayList<>(), new HashMap<>());
    static ArrayList<Integer> dStoreList = new ArrayList<>();
    static HashMap<String, ArrayList<Integer>> filesList = new HashMap<>();

    static HashMap<String, PrintWriter> storeFilePW = new HashMap<>();
    static HashMap<String, PrintWriter> removeFilePW = new HashMap<>();

    static int rebalanceCounter = 0;

    static int rebalanceCompleteCounter = 0;

    static boolean inRebalance = false;

    static int rep_factor = 0;

    static int completed_stores = 0;

    static int completed_deletes = 0;

    static int loadPortCounter = 0;

    public static void main(String [] args){

        /*
        final int cport = Integer.parseInt(args[0]);
        final int rep_factor = Integer.parseInt(args[1]);
        final int timeout = Integer.parseInt(args[2]);
        final int reb_period = Integer.parseInt(args[3]);
         */

        final int cport = 12345;
        final int rep_factor = 1;
        setRep_factor(rep_factor);
        final int timeout = 5000;
        final int reb_period = 15000;

        index.clear();
        dStoreList.clear();

        startRebalancePeriod(reb_period * 1000);
        try{
            ServerSocket ss = new ServerSocket(cport);
            System.out.println("Controller is listening on port " + cport);
            for(;;){
                try{
                    System.out.println("Waiting for connection!");
                    Socket client = ss.accept();
                    System.out.println("Connected!");
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            int currentPort = 0;
                            try {
                                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                boolean isDStore = false;
                                String line;
                                while ((line = in.readLine()) != null) {
                                    String[] contents = line.split(" ");
                                    String command = contents[0];
                                    System.out.println(command + " command");
                                    if (command.equals("JOIN")) {
                                        isDStore = true;
                                        currentPort = Integer.parseInt(contents[1]);
                                        addDStore(contents[1]);
                                        rebalance();
                                    } else if (command.equals("LIST") && !isDStore) {
                                        if (hasEnoughDStores(out)){
                                            listFiles(out);
                                            System.out.println("Client LIST");
                                        }
                                    } else if (command.equals("LIST") && isDStore){
                                        rebalanceCounter++;
                                        System.out.println("DStore LIST");
                                        ArrayList<String> fileNames = new ArrayList<>();
                                        for(int i = 1; i < contents.length; i++){
                                            fileNames.add(contents[i]);
                                        }
                                        updateHashMapWith(currentPort, fileNames);
                                    } else if (command.equals("STORE")) {
                                        if(hasEnoughDStores(out)) {
                                            if(!index.fileStatus.containsKey(contents[1])) {
                                                storeFilePW.put(contents[1], out);
                                            }
                                            store(out, rep_factor, contents[1], Integer.parseInt(contents[2]));
                                        }
                                    } else if (command.equals("STORE_ACK") && index.fileStatus.get(contents[1]) == 1001) {
                                        storeComplete(currentPort, storeFilePW.get(contents[1]), getCompleted_stores() + 1, contents[1]);
                                    } else if (command.equals("LOAD")){
                                        if (hasEnoughDStores(out)) {
                                            if(index.file_names.contains(contents[1])) {
                                                setLoadPortCounter(1);
                                                load(out, contents[1], getLoadPortCounter());
                                            } else {
                                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                            }
                                        }
                                    } else  if (command.equals("RELOAD")){
                                        if (getLoadPortCounter() == filesList.get(contents[1]).size()-1) {
                                            out.println("ERROR_LOAD");
                                        } else if (hasEnoughDStores(out)){
                                            if(index.file_names.contains(contents[1])) {
                                                setLoadPortCounter(getLoadPortCounter()+1);
                                                load(out, contents[1], getLoadPortCounter());
                                            } else {
                                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                            }
                                        }
                                    } else if (command.equals("REMOVE")){
                                        if (hasEnoughDStores(out)){
                                            if(index.file_names.contains(contents[1])) {
                                                removeFilePW.put(contents[1], out);
                                            }
                                            remove(out, contents[1]);
                                        }
                                    } else if (command.equals("REMOVE_ACK") && index.fileStatus.get(contents[1]) == 2001){
                                        if (hasEnoughDStores(out)){
                                            removeComplete(currentPort, removeFilePW.get(contents[1]), getCompleted_deletes() + 1, contents[1]);
                                        }
                                    } else if (command.equals("REBALANCE_COMPLETE")) {
                                        out.println("LIST");
                                        setRebalanceCompleteCounter(getRebalanceCompleteCounter()-1);
                                        if(getRebalanceCompleteCounter() == 0){
                                            fixIndexAfterRebalance();
                                        }
                                    } else {
                                        System.out.println("INVALID MESSAGE: " + line);
                                    }
                                }
                                client.close();
                            } catch (SocketException e) {
                                System.out.println("SocketException " + e);
                                fixFailedDStore(currentPort);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();
                } catch (SocketTimeoutException e){
                    System.out.println("error "+e);
                } catch(Exception e){
                    System.out.println("error "+e);
                }
            }
        } catch(Exception e){System.out.println("error "+e);}
    }

    private synchronized static boolean hasEnoughDStores(PrintWriter printWriter){
        if(!(dStoreList.size() >= rep_factor)){
            printWriter.println("ERROR_NOT_ENOUGH_DSTORES");
            return false;
        }
        return true;
    }

    private synchronized static void addDStore(String port){
        dStoreList.add(Integer.valueOf(port));
        System.out.println("DStore added!");
    }
    private synchronized static void listFiles(PrintWriter printWriter){
        String file_names = "";
        if(index.getFile_names().size() > 0) {
            for(String file : index.getFile_names()){
                file_names = file_names + " " + file;
            }
        }

        printWriter.println("LIST" + file_names);
        System.out.println("Successfully LISTed!");
    }

    private synchronized static void store (PrintWriter printWriter, int rep_factor, String fileName, int fileSize){
        if(!index.fileStatus.containsKey(fileName)) {
            ArrayList<Integer> temp = new ArrayList<>();
            temp.add(fileSize);
            filesList.put(fileName, temp);
            index.fileStatus.put(fileName, 1001); //index updated to "store in progress"
            System.out.println("Index updated to \"Store in progress\"!");
            String ports = "";
            for (int i = 0; i < rep_factor; i++) {
                ports = ports + " " + dStoreList.get(i);
            }
            printWriter.println("STORE_TO" + ports);
            System.out.println("Store message sent!");
        } else {
            printWriter.println("ERROR_FILE_ALREADY_EXISTS");
            System.out.println("ERROR_FILE_ALREADY_EXISTS");
        }

    }

    private synchronized static void storeComplete(int port, PrintWriter printWriter, int completed_stores, String fileName) {
        if(completed_stores == getRep_factor()) {
            index.file_names.add(fileName);
            System.out.println(fileName + " added to the Index!");
            setCompleted_stores(0);

            if(!filesList.get(fileName).contains(port)) {
                filesList.get(fileName).add(port);
                filesList.put(fileName, filesList.get(fileName));
            }

            index.fileStatus.remove(fileName);
            index.fileStatus.put(fileName, 1000); //index updated to "store complete"
            System.out.println("Index updated to \"Store complete\"!");
            printWriter.println("STORE_COMPLETE");
            System.out.println("STORE completed!");
        } else {
            setCompleted_stores(getCompleted_stores()+1);
            if(!filesList.get(fileName).contains(port)) {
                filesList.get(fileName).add(port);
                filesList.put(fileName, filesList.get(fileName));
            }
        }
    }

    private synchronized static void load(PrintWriter printWriter, String fileName, int loadPortCounter){
        for(String key : filesList.keySet()){
            if(key.equals(fileName) && index.file_names.contains(key)){
                int fileSize = filesList.get(fileName).get(0);
                int port = filesList.get(fileName).get(loadPortCounter);
                System.out.println("LOAD_FROM " + port + " " + fileSize);
                printWriter.println("LOAD_FROM " + port + " " + fileSize);
            }
        }
    }

    private synchronized static void remove(PrintWriter printWriter, String fileName){
        if(index.file_names.contains(fileName)) {
            index.fileStatus.remove(fileName);
            index.fileStatus.put(fileName, 2001); //remove in progress
            System.out.println("Index updated to \"Remove in progress\"");
            if (filesList.containsKey(fileName)) {
                ArrayList<Integer> dStores = filesList.get(fileName);
                for (int i = 1; i < dStores.size(); i++) {
                    sendRemove(dStores.get(i), fileName);
                }
            }
        } else {
            printWriter.println("ERROR_FILE_DOES_NOT_EXIST");
            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
        }
    }

    private synchronized static void sendRemove(Integer port, String fileName){
        try {
            Socket dStore = new Socket(InetAddress.getLoopbackAddress(), port);
            PrintWriter dStorePW = new PrintWriter(dStore.getOutputStream(), true);
            dStorePW.println("REMOVE " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized static void removeComplete(int port, PrintWriter printWriter, int completed_deletes, String fileName) {
        if (completed_deletes == filesList.get(fileName).size() - 1) {
            index.file_names.remove(fileName);
            setCompleted_deletes(0);

            filesList.remove(fileName);

            index.fileStatus.remove(fileName);
            index.fileStatus.put(fileName, 2000); //index updated to "remove complete"
            System.out.println("Index updated to \"Remove complete\"!");
            printWriter.println("REMOVE_COMPLETE");
            System.out.println("Remove completed!");
        } else {
            setCompleted_deletes(getCompleted_deletes() + 1);

            filesList.get(fileName).remove(port);
        }
    }

    private synchronized static void startRebalancePeriod(int rebalance_period){
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                rebalance();
            }
        }, rebalance_period, rebalance_period);
    }

    private synchronized static void rebalance(){
        sendRebalanceLists();
        System.out.println("Entered rebalance");
    }

    private synchronized static void sendRebalanceLists(){
        fixHashMap();
        if(dStoreList.size() > 0){
            inRebalance = true;
        }
        for(int i = 0; i < dStoreList.size(); i++){
            try {
                Socket dStore = new Socket(InetAddress.getLoopbackAddress(), dStoreList.get(i));
                PrintWriter dStorePW = new PrintWriter(dStore.getOutputStream(), true);
                dStorePW.println("LIST");
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("LIST sent to DStore");
        }
    }

    private synchronized static void updateHashMapWith(Integer port, ArrayList<String> fileNames) {
        System.out.println("Entered Update HM");
        if (filesList.size() != 0) {
            for (String f : fileNames) {
                if(filesList.containsKey(f)) {
                    filesList.get(f).add(port);
                }
            }
        }
        if (rebalanceCounter >= dStoreList.size()) {
            System.out.println("Finished updating HM, proceeding to calc reb!");
            rebalanceCounter = 0;
            calculateRebalances();
        }
    }

    private synchronized static void fixHashMap(){
        for(String key : filesList.keySet()) {
            int tempSize = filesList.get(key).get(0);
            filesList.get(key).clear();
            filesList.get(key).add(tempSize);
        }
    }

    private synchronized static void calculateRebalances(){
        String minFile;
        Integer minDStore;
        String maxFile;
        Integer maxDStore;
        HashMap<Integer, ArrayList<String>> dStoreHM = getDStoreHM();

        Double minRep = Math.floor(Double.valueOf(rep_factor* filesList.size())/Double.valueOf(dStoreList.size()));
        Double maxRep = Math.ceil(Double.valueOf(rep_factor* filesList.size())/Double.valueOf(dStoreList.size()));

        HashMap<Integer, ArrayList<String>> fileToStoreHM = new HashMap<>();
        HashMap<Integer, ArrayList<String>> fileToRemoveHM = removeBuggedFiles(index.fileStatus);
        HashMap<String, ArrayList<Integer>> fileToSendHM = new HashMap<>();

        while(!checkIfFinished(dStoreHM, rep_factor, minRep, maxRep)) {
            while (!areDStoreMaxRep(maxRep, dStoreHM)) {
                Integer skip = 0;
                minDStore = getMinDStore(dStoreHM);
                minFile = getMinFile(skip, dStoreHM);
                while (dStoreHM.get(minDStore).contains(minFile)) {
                    skip++;
                    minFile = getMinFile(skip, dStoreHM);
                }

                dStoreHM.get(minDStore).add(minFile);

                if (fileToStoreHM.containsKey(minDStore)) {
                    if (!fileToStoreHM.get(minDStore).contains(minFile)) {
                        fileToStoreHM.get(minDStore).add(minFile);
                    }
                } else {
                    fileToStoreHM.put(minDStore, new ArrayList<String>(Arrays.asList(minFile)));
                }

                if (fileToSendHM.containsKey(minFile)) {
                    if (!fileToSendHM.get(minFile).contains(minDStore)) {
                        fileToSendHM.get(minFile).add(minDStore);
                    }
                } else {
                    if(filesList.get(minFile) != null) {
                        fileToSendHM.put(minFile, new ArrayList<Integer>(Arrays.asList(filesList.get(minFile).get(1), minDStore)));
                    }
                }
            }
            if (!checkIfFinished(dStoreHM, rep_factor, minRep, maxRep)) {
                while(!areFilesMinRep(rep_factor, dStoreHM)){
                    maxDStore = getMaxDStore(dStoreHM);
                    maxFile = getMaxFile(dStoreHM, maxDStore);

                    if (fileToRemoveHM.containsKey(maxDStore)) {
                        if (!fileToRemoveHM.get(maxDStore).contains(maxFile)) {
                            fileToRemoveHM.get(maxDStore).add(maxFile);
                        }
                    } else {
                        fileToRemoveHM.put(maxDStore, new ArrayList<String>(Arrays.asList(maxFile)));
                    }

                    dStoreHM.get(maxDStore).remove(maxFile);

                }
            }
        }
        prepareRebalanceRequests(fileToStoreHM, fileToRemoveHM, fileToSendHM);
    }

    private synchronized static HashMap<Integer, ArrayList<String>> removeBuggedFiles(HashMap<String, Integer> statusHM){
        HashMap<Integer, ArrayList<String>> filesToRemove = new HashMap<>();
        for(String key : statusHM.keySet()){
            if(statusHM.get(key) == 2001) {
                ArrayList<Integer> ports = filesList.get(key);
                ports.remove(0);
                for(Integer port : ports){
                    if (filesToRemove.containsKey(port)) {
                        if (!filesToRemove.get(port).contains(key)) {
                            filesToRemove.get(port).add(key);
                        }
                    } else {
                        filesToRemove.put(port, new ArrayList<String>(Arrays.asList(key)));
                    }
                }
            }
        }
        return filesToRemove;
    }

    private synchronized static HashMap<Integer, ArrayList<String>> getDStoreHM(){
        for(String key : index.file_names){
            if(index.fileStatus.get(key) == 2001) {
                filesList.remove(key);
            }
        }
        HashMap<Integer, ArrayList<String>> dStoreHM = new HashMap<>();
        for(String key : filesList.keySet()){
            for(int i = 1; i < filesList.get(key).size(); i++){
                if(dStoreHM.containsKey(filesList.get(key).get(i))){
                    if (!dStoreHM.get(filesList.get(key).get(i)).contains(key)){
                        dStoreHM.get(filesList.get(key).get(i)).add(key);
                    }
                } else {
                    ArrayList<String> tempValue = new ArrayList<>();
                    tempValue.add(key);
                    dStoreHM.put(filesList.get(key).get(i), tempValue);
                }
            }
        }
        for(Integer port : dStoreList){
            if(!dStoreHM.containsKey(port)){
                dStoreHM.put(port, new ArrayList<>());
            }
        }
        return dStoreHM;
    }

    private synchronized static boolean areDStoreMaxRep(Double maxRep, HashMap<Integer, ArrayList<String>> hm){
        for(Integer key : hm.keySet()){
            if(hm.get(key).size() < maxRep){
                return false;
            }
        }
        return true;
    }

    private static Integer getMinDStore(HashMap<Integer, ArrayList<String>> dStoreHM){
        Integer minDStore = 1;
        Integer minDStoreSize = Integer.MAX_VALUE;
        for(Integer dStore : dStoreHM.keySet()){
            if(dStoreHM.get(dStore).size() < minDStoreSize){
                minDStore = dStore;
                minDStoreSize = dStoreHM.get(dStore).size();
            }
        }
        return minDStore;
    }

    private static Integer getMaxDStore(HashMap<Integer, ArrayList<String>> dStoreHM){
        Integer maxDStore = 1;
        Integer maxDStoreSize = Integer.MIN_VALUE;
        for(Integer dStore : dStoreHM.keySet()){
            if(dStoreHM.get(dStore).size() > maxDStoreSize){
                maxDStore = dStore;
                maxDStoreSize = dStoreHM.get(dStore).size();
            }
        }
        return maxDStore;
    }

    private static String getMinFile(Integer skip, HashMap<Integer, ArrayList<String>> hm){
        HashSet<String> filesSet = new HashSet<>();
        for(ArrayList<String> fileNames : hm.values()){
            filesSet.addAll(fileNames);
        }
        String minFile = getAbsMinFile(filesSet, revertToKeyFileName(hm));
        while(skip > 0){
            skip--;
            filesSet.remove(minFile);
            minFile = getAbsMinFile(filesSet, revertToKeyFileName(hm));
        }
        return minFile;
    }

    private static String getMaxFile(HashMap<Integer, ArrayList<String>> hm, Integer maxDStore){
        Integer tempMax = Integer.MIN_VALUE;
        String maxFile = "";
        ArrayList<String> fileArray = hm.get(maxDStore);
        HashMap<String, ArrayList<Integer>> invertedHM = revertToKeyFileName(hm);
        for(String key : fileArray){
            if(invertedHM.get(key).size() > tempMax){
                tempMax = invertedHM.get(key).size();
                maxFile = key;
            }
        }
        return maxFile;
    }

    private static String getAbsMinFile(Set<String> stringSet, HashMap<String, ArrayList<Integer>> hm){
        String minFile = "";
        Integer minFileRep = Integer.MAX_VALUE;
        for(String key : stringSet){
            if (hm.get(key).size() < minFileRep){
                minFile = key;
                minFileRep = hm.get(key).size();
            }
        }
        return minFile;
    }

    private static HashMap<String, ArrayList<Integer>> revertToKeyFileName(HashMap<Integer, ArrayList<String>> hm){
        HashMap<String, ArrayList<Integer>> reverted = new HashMap<>();
        for(Integer key : hm.keySet()){
            for(int i = 0; i < hm.get(key).size(); i++){
                if(reverted.containsKey(hm.get(key).get(i))){
                    if (!reverted.get(hm.get(key).get(i)).contains(key)){
                        reverted.get(hm.get(key).get(i)).add(key);
                    }
                } else {
                    reverted.put(hm.get(key).get(i), new ArrayList<Integer>(Arrays.asList(key)));
                }
            }
        }
        return reverted;
    }

    private static boolean checkIfFinished(HashMap<Integer, ArrayList<String>> dStoreHM, Integer rep_factor, Double minRep, Double maxRep){
        HashMap<String, ArrayList<Integer>> fileHM = revertToKeyFileName(dStoreHM);
        for(String key : fileHM.keySet()){
            if(!(fileHM.get(key).size() == rep_factor)){
                return false;
            }
        }
        for(Integer key : dStoreHM.keySet()){
            if(!((dStoreHM.get(key).size()) >= minRep && (dStoreHM.get(key).size()) <= maxRep)){
                return false;
            }
        }
        return true;
    }

    private static boolean areFilesMinRep(Integer rep_factor, HashMap<Integer, ArrayList<String>> hm){
        HashMap<String, ArrayList<Integer>> fileHM = revertToKeyFileName(hm);
        for(String key : fileHM.keySet()){
            if(fileHM.get(key).size() > rep_factor){
                return false;
            }
        }
        return true;
    }

    private synchronized static void prepareRebalanceRequests(HashMap<Integer, ArrayList<String>> fileToStoreHM, HashMap<Integer, ArrayList<String>> fileToRemoveHM, HashMap<String, ArrayList<Integer>> fileToSendHM){
        if(!fileToStoreHM.isEmpty() || !fileToRemoveHM.isEmpty() || !fileToSendHM.isEmpty()) {
            HashMap<HashMap<Integer, String>, ArrayList<Integer>> finalSend = new HashMap<>();

            for (String file : fileToSendHM.keySet()) {
                HashMap<Integer, String> linkHM = new HashMap<>();
                linkHM.put(fileToSendHM.get(file).get(0), file);
                ArrayList<Integer> portsToSend = fileToSendHM.get(file);
                portsToSend.remove(0);
                finalSend.put(linkHM, portsToSend);
            }

            setRebalanceCompleteCounter(0);
            for (Integer port : dStoreList) {
                String files_to_send = "";
                String files_to_remove = "";
                Integer filesSendCounter = 0;

                ArrayList<HashMap<Integer, String>> arrayHM = new ArrayList<>(finalSend.keySet());

                for (int i = 0; i < arrayHM.size(); i++) {
                   for(Integer innerPort : arrayHM.get(i).keySet()){
                       if(innerPort.equals(port)){
                           filesSendCounter++;
                           files_to_send += " " + arrayHM.get(i).get(port) + " " + finalSend.get(arrayHM.get(i)).size() + getPortsToMessage(finalSend.get(arrayHM.get(i)));
                       }
                   }
                }

                files_to_send = filesSendCounter + files_to_send;
                files_to_remove = (fileToRemoveHM.get(port) == null ? "0" : (fileToRemoveHM.get(port).size() + getFilesToMessage(fileToRemoveHM.get(port))));
                sendRebalanceRequest(port, files_to_send, files_to_remove);
            }

        } else {
            System.out.println("No rebalance needed!");
        }
    }

    private static String getFilesToMessage(ArrayList<String> filesList) {
        String output = "";
        for (String file : filesList) {
            output += " " + file;
        }

        return output;
    }

    private static String getPortsToMessage(ArrayList<Integer> portsList) {
        String output = "";
        for (Integer port : portsList) {
            output += " " + port.toString();
        }

        return output;
    }

    private static void sendRebalanceRequest(Integer port, String files_to_send, String files_to_remove){
        System.out.println("REBALANCE " + files_to_send + " " + files_to_remove);
        try {
            Socket dStore = new Socket(InetAddress.getLoopbackAddress(), port);
            PrintWriter dStorePW = new PrintWriter(dStore.getOutputStream(), true);
            dStorePW.println("REBALANCE " + files_to_send + " " + files_to_remove);
            setRebalanceCompleteCounter(getRebalanceCompleteCounter()+1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized static void fixIndexAfterRebalance() {
        for(String key : index.fileStatus.keySet()){
            if(index.fileStatus.get(key) == 2001){
                index.fileStatus.remove(key);
                index.file_names.remove(key);
            }
        }
    }

    private synchronized static void fixFailedDStore(Integer port) {
        if(port == 0) return;
        dStoreList.remove(port);
        for(String key : filesList.keySet()){
            if(filesList.get(key).contains(port)){
                filesList.get(key).remove(port);
                if(filesList.get(key).size() == 1){
                    filesList.remove(key);
                    index.file_names.remove(key);
                    index.fileStatus.remove(key);
                }
            }
        }
    }


//Getters and Setters ---------------------------------------------------------------------------------------------------------------

    public static int getRep_factor() {
        return rep_factor;
    }

    public static void setRep_factor(int rep_factor) {
        Controller.rep_factor = rep_factor;
    }

    public static int getCompleted_stores() {
        return completed_stores;
    }

    public static void setCompleted_stores(int completed_stores) {
        Controller.completed_stores = completed_stores;
    }

    public static int getLoadPortCounter() {
        return loadPortCounter;
    }

    public static void setLoadPortCounter(int loadPortCounter) {
        Controller.loadPortCounter = loadPortCounter;
    }

    public static int getCompleted_deletes() {
        return completed_deletes;
    }

    public static void setCompleted_deletes(int completed_deletes) {
        Controller.completed_deletes = completed_deletes;
    }

    public static int getRebalanceCompleteCounter() {
        return rebalanceCompleteCounter;
    }

    public static void setRebalanceCompleteCounter(int rebalanceCompleteCounter) {
        Controller.rebalanceCompleteCounter = rebalanceCompleteCounter;
    }
}
