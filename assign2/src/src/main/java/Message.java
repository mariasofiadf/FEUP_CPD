import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Message {
    public String assembleMsg(Map<String,String>map){
        StringBuilder msg = new StringBuilder();
        map.forEach((k,v) ->{

            if(k.equalsIgnoreCase("body")){
                msg.append("\n\n");
                msg.append(v);
            }
            else{
                msg.append(k + " " + v + "\n");
            }
        }
        );
        return msg.toString();
    }


    public Map<String, String> disassembleMsb(String msg) throws Exception {
        Map<String, String> map = new HashMap<>();
        Scanner scanner = new Scanner(msg);
        int emptyLineCount = 0;
        while (scanner.hasNextLine() && emptyLineCount < 2) {
            String line = scanner.nextLine();  //i.e. line = "event join"
            String[] splited = line.split("\\s+");
            if(splited.length > 2)
                throw new Exception("Disassemble Message Error: Too many words in Header lines");
            if(line.isEmpty()){
                emptyLineCount++;
                continue;
            }
            map.put(splited[0],splited[1]); //i.e. "event", "join"
        }
        StringBuilder body = new StringBuilder();
        while (scanner.hasNextLine() && emptyLineCount == 2){
            String line = scanner.nextLine();
            body.append(line).append("\n");
        }
        map.put("body", body.toString());
        scanner.close();
        return  map;
    }

}