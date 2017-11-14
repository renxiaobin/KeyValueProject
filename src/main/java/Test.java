import java.io.File;
import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        File STORE_FILE = new File("/Users/renxiaobin/opt/localdisk／store.txt");
        STORE_FILE.deleteOnExit();
        boolean created = STORE_FILE.createNewFile();
        if (!created) {
            System.out.println("Cannot create store file on local disk!");

        }else{
            System.out.println("args = [" + created + "]");
        }
    }
}
