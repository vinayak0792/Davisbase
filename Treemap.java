import java.util.*;

public class Treemap {
   public static void main(String[] args) {
   // creating tree map 
   TreeMap<Integer, String> treemap = new TreeMap<Integer, String>();
      
   // populating tree map
   treemap.put(2, "two");
   treemap.put(1, "one");
   treemap.put(3, "three");
   treemap.put(6, "six");
   treemap.put(5, "five");   
      
   // Putting value at key 3
   System.out.println("Value before modification: "+ treemap);            
   System.out.println("Value returned: "+ treemap.put(3,"TP"));      
   System.out.println("Value after modification: "+ treemap);
   }     
}