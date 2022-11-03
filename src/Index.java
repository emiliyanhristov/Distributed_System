import java.util.ArrayList;
import java.util.HashMap;

public class Index {

    ArrayList<String> file_names;
    HashMap<String, Integer> fileStatus;

    public Index (ArrayList<String> file_names, HashMap<String, Integer> fileStatus){
        this.file_names = file_names;
        this.fileStatus = fileStatus;
    }

    public ArrayList<String> getFile_names() {
        return file_names;
    }

    public void setFile_names(ArrayList<String> file_names) {
        this.file_names = file_names;
    }

    public HashMap<String, Integer> getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(HashMap<String, Integer> fileStatus) {
        this.fileStatus = fileStatus;
    }

    public void clear(){
        this.file_names.clear();
        this.fileStatus.clear();
    }
}
