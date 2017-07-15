import java.io.BufferedReader;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Set;

/**
 *  @author Meghana Pochiraju
 *  @version 1.0
 *
 */
public class DavisBasePromptExample{
    public static void main(String args[])
    {
        Davisbase obj = new Davisbase();
        obj.mainmethod();
    }
}
class Davisbase {

	
	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "Â©2016 Chris Irwin Davis";
	static boolean isExit = false;
	static int pageSize = 512;  
        TreeMap<String,BPlusTree> Davisbase_table_trees = new TreeMap<String,BPlusTree>();
        HashMap<String,Integer> last_primarykey = new HashMap<String, Integer>();
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public void mainmethod() {

                getState();
		splashScreen();
		String userCommand = ""; 
                
		while(!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
                saveState();

	}

	public void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}

	public String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}

		public void help() {
			System.out.println(line("*",80));
			System.out.println("SUPPORTED COMMANDS");
			System.out.println("All commands below are case insensitive");
			System.out.println();
			System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
			System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
			System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
			System.out.println("\tVERSION;                                         Show the program version.");
			System.out.println("\tHELP;                                            Show this help information");
			System.out.println("\tEXIT;                                            Exit the program");
			System.out.println();
			System.out.println();
			System.out.println(line("*",80));
		}


	public String getVersion() {
		return version;
	}
	
	public String getCopyright() {
		return copyright;
	}
	
	public void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public void parseUserCommand (String userCommand) {

		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
			case "select":
				parseQueryString(userCommand);
				break;
			case "drop":
				System.out.println("STUB: Calling your method to drop items");
				parseDropTables(userCommand);
				break;
			case "create":
				parseCreateString(userCommand);
				break;
                        case "show":
                                parseShowTables(userCommand);
                                break;
                        case "insert":
                                parseInsertString(userCommand);
                                break;
                        case "delete":
                                parseDeleteString(userCommand);
                                break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}


	public void parseQueryString(String queryString) {

                ArrayList<String> selectTableTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
                String tableName = selectTableTokens.get(3);
                ArrayList<String> col_names = new ArrayList<String>();
                ArrayList<String> data_type = new ArrayList<String>();
                ArrayList<String> nullable = new ArrayList<String>();
                readFromDavisbase_columns(tableName, col_names, data_type, nullable);
                
                ArrayList<String> values = new ArrayList<String>();
                        if(queryString.contains("where"))
                        {
                            String whereclause = queryString.substring(queryString.indexOf("where")+"where".length());
                            String col_name = whereclause.split("=")[0].trim();
                            String value = whereclause.split("=")[1].trim();
                           // System.out.println("value = "+value);
                           // System.out.println("first col"+col_names.get(0));
                            ArrayList<Integer> rowids = new ArrayList<Integer>();
                            if(col_name.equals(col_names.get(0)))
                            {
                                rowids.add(Integer.parseInt(value));
                            }
                            else
                            {
                                try {
                                    RandomAccessFile ndxFile;
                                 
                                if(tableName.trim().equals("davisbase_tables")|| tableName.trim().equals("davisbase_columns"))
                                {
                                   // System.out.println("in catalog");
                                    ndxFile = new RandomAccessFile("data/catalog/"+col_name+".ndx","rw");
                                }
                                else
                                {
                                   // System.out.println("in user_data");
                                     ndxFile = new RandomAccessFile("data/user_data/"+tableName+"/"+col_name+".ndx","rw");
                                }
                                    while(ndxFile.getFilePointer()<ndxFile.length())
                                    {
                                        
                                        int rowid = ndxFile.readInt();
                                        String val = ndxFile.readLine();
                                       // System.out.println("ndx val = "+val);
                                        if(val.equals(value))
                                        {
                                            rowids.add(rowid);
                                        }
                                    }
                                } catch (Exception e) {
                                }
                            }
                            try{
                                RandomAccessFile tableFile;
                                if(tableName.trim().equals("davisbase_tables")||tableName.trim().equals("davisbase_columns"))
                                {
                                   // System.out.println("in catalog");
                                    tableFile = new RandomAccessFile("data/catalog/"+tableName+".tbl","rw");
                                }
                                else
                                {
                                   // System.out.println("in user_data");
                                     tableFile = new RandomAccessFile("data/user_data/"+tableName+"/"+tableName+".tbl","rw");
                                }
                              
                                for(int i = 0; i< rowids.size();i++)
                                {
                               
                                    String s="";
                                    int position = Davisbase_table_trees.get(tableName).searchKey(rowids.get(i));
                                    tableFile.seek(position);
                                    for(int j =0 ; j < col_names.size();j++)
                                    {
                                        if(data_type.get(j).equals("tinyint"))
                                        {
                                            byte val = tableFile.readByte();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("smallint"))
                                        {
                                           short val = tableFile.readShort();
                                           if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("int"))
                                        {
                                            int val = tableFile.readInt();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("bigint"))
                                        {
                                            long val = tableFile.readLong();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("real"))
                                        {
                                            float val = tableFile.readFloat();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                            
                                        }
                                        else if(data_type.get(j).equals("double"))
                                        {
                                            double val = tableFile.readDouble();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("date")||data_type.get(j).equals("datetime"))
                                        {
                                            Long dateT = tableFile.readLong();
                                            if(dateT == -1){
                                                s+="null,";
                                            }
                                            else{
					    Date dateTime = new Date(dateT);
					    SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
					    format.format(dateTime);
                                            s+=dateTime.getTime()+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("text"))
                                        {
                                            int size = tableFile.readByte();
                                            byte[] val = new byte[size];
                                            tableFile.read(val);
                                            String ss = new String(val);
                                                s+=ss+",";
                                        }
                                        
                                    } 
                                    values.add(s);
                                }
                           
                            
                            }
                            catch(Exception e)
                            {
                                //System.out.println(e);
                            }  
                        }
                        else
                        { 
                            for (int i=1; i<= last_primarykey.get(tableName) ; i++)
                                {
                                try{
                                RandomAccessFile tableFile;
                                if(tableName.trim().equals("davisbase_tables")||tableName.trim().equals("davisbase_columns"))
                                {
                                   // System.out.println("in catalog");
                                    tableFile = new RandomAccessFile("data/catalog/"+tableName+".tbl","rw");
                                }
                                else
                                {
                                    //System.out.println("in user_data");
                                     tableFile = new RandomAccessFile("data/user_data/"+tableName+"/"+tableName+".tbl","rw");
                                }
                                   // System.out.println("i==="+i);
                                    String s="";
                                    int position = Davisbase_table_trees.get(tableName).searchKey(i);
                                    //System.out.println(Davisbase_table_trees.get(tableName).searchKey(i));
                                    tableFile.seek(position);//
                                    for(int j =0 ; j < col_names.size();j++)
                                    {
                                        if(data_type.get(j).equals("tinyint"))
                                        {
                                            byte val = tableFile.readByte();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("smallint"))
                                        {
                                           short val = tableFile.readShort();
                                           if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("int"))
                                        {
                                            int val = tableFile.readInt();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("bigint"))
                                        {
                                            long val = tableFile.readLong();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("real"))
                                        {
                                            float val = tableFile.readFloat();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                            
                                        }
                                        else if(data_type.get(j).equals("double"))
                                        {
                                            double val = tableFile.readDouble();
                                            if(val == -1){
                                                s+="null,";
                                            }
                                            else
                                            {
                                                s+=val+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("date")||data_type.get(j).equals("datetime"))
                                        {
                                            Long dateT = tableFile.readLong();
                                            if(dateT == -1){
                                                s+="null,";
                                            }
                                            else{
					    Date dateTime = new Date(dateT);
					    SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
					    format.format(dateTime);
                                            s+=dateTime.getTime()+",";
                                            }
                                        }
                                        else if(data_type.get(j).equals("text"))
                                        {
                                            int size = tableFile.readByte();
                                            byte[] val = new byte[size];
                                            tableFile.read(val);
                                            String ss = new String(val);
                                                s+=ss+",";
                                        }
                                    }
                            
                                    
                                    values.add(s);
                                   
                                }
                           catch(Exception e)
                            {
                                //System.out.println(e);
                            }
                            
                            }
                              
                        } //else
//                        for(int i =0; i < values.size();i++)
//                        {
//                            System.out.println(values.get(i));
//                        }
                        if(selectTableTokens.get(1).equals("*"))
                        {
                                for(String colss:col_names)
                                {
                                    System.out.print(colss+"        ");
                                    
                                }System.out.println();
                               System.out.println("------------------------------");
                                
                             for(int i =0; i < values.size();i++)
                            {
                                String[] spl = values.get(i).split(",");
                                for (String valss:spl)
                                {
                                    System.out.print(valss+"        ");
                                }
                               
                                System.out.println();
                            }
                        }
                        
                        else
                        {
                                      // print selected columns 
                            String[] neededcols = queryString.substring(queryString.indexOf("select")+"select".length()+1,queryString.indexOf("from")).split(",");
                            for(int i = 0; i < neededcols.length; i++)
                            {
                                System.out.print(neededcols[i]+"        ");
                            }
                            System.out.println();
                            System.out.println("--------------------------------");
                            for(int m = 0; m < values.size(); m++)
                            {
                                //System.out.println(values.get(m));
                                for(int i =0; i < col_names.size(); i++)
                                {
                                    //System.out.println(i);
                                    for(int j =0; j<neededcols.length;j++)
                                    {
                                       // System.out.println(j);
                                        if(neededcols[j].trim().equals(col_names.get(i)))
                                        {
                                            //System.out.println(values.get(m));
                                            
                                            System.out.print(values.get(m).trim().split(",")[i]+"       ");
                                        }
                                        
                                    }
                                    //System.out.println(i);
                                }
                                System.out.println();
                            }
                            
                            
                            
                        }
                
	}

	public void parseCreateString(String createTableString) {
		
		if(validateCreateString(createTableString))
                {
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
                String tablename = createTableTokens.get(2);
		String tableFileName = createTableTokens.get(2) + ".tbl";
                File dir2 = new File("data\\user_data\\"+tablename);
                dir2.mkdir();
		try {
                        
			RandomAccessFile tableFile = new RandomAccessFile("data/user_data/"+tablename+"/"+tableFileName, "rw");
			tableFile.setLength(pageSize);
                        Davisbase_table_trees.put(tablename, new BPlusTree());
       
		}
		catch(Exception e) {
			
		}
		
                
                HashMap<Integer,String[]> columns = parseColumnNames(createTableString);
                String[] col_names = new String[columns.size()];
                String[] data_types = new String[columns.size()];
                String[] isNull = new String[columns.size()];
                for(int i =0; i < columns.size(); i++)
                {
                   col_names[i] = columns.get(i+1)[0];
                   data_types[i] = columns.get(i+1)[1];
                   isNull[i] = columns.get(i+1)[2];
                }
                String primary_key = col_names[0];
//                for(int i =0; i<columns.size();i++)
//                {
//                    System.out.println(col_names[i]);
//                    System.out.println(data_types[i]);
//                    System.out.println(isNull[i]);
//                }
                
                writeToDavisbase_tables(tablename);
                writeToDavisbase_columns(tablename,col_names,data_types,isNull);
                ndxfiles(tablename, col_names);
                }
                else
                {
                    System.out.println("Wrong syntax");
                }
	}
       public void parseInsertString(String insertTableString)
        {
            boolean flag = true;
            ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(insertTableString.split(" ")));
            ArrayList<Integer> primary_values = new ArrayList<Integer>();
                String tablename = createTableTokens.get(2);
                String tableFileName = createTableTokens.get(2) + ".tbl";
                if(last_primarykey.containsKey(tablename)){
                for(int i =1; i<=last_primarykey.get(tablename);i++)
                    {

                        try{
                            RandomAccessFile tb = new RandomAccessFile("data/user_data/"+tablename+"/"+tablename+".tbl", "rw");
                            int pos = Davisbase_table_trees.get(tablename).searchKey(i);
                            tb.seek(pos);
                            int rowid = tb.readInt();
                            primary_values.add(rowid);
                        }
                        catch(Exception ex)
                        {

                        }
                    }
                }

                try{
                    RandomAccessFile davisbase_tables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
                    for(int i =1; i<=last_primarykey.get("davisbase_tables");i++)
                    {
                        
                        try{
                            int pos = Davisbase_table_trees.get("davisbase_tables").searchKey(i);
                            davisbase_tables.seek(pos);
                            int rowid = davisbase_tables.readInt();
                            int tablename_size = davisbase_tables.readByte();
                            byte[] s = new byte[tablename_size];
                            davisbase_tables.read(s);
                            String tablenm = new String(s);
                            if(tablenm.equals(tablename))
                            {
                                flag=false;
                                break;
                            }
                            
                        }
                        catch(NullPointerException e)
                        {
                            
                        } catch (IOException ex) {
                            
                        }
                        
                    }
                    
                    if(flag)
                    {
                        System.out.println("Insert cannot be performed");
                    }
                    else
                    {

                        ArrayList<String> col_names = new ArrayList<String>();
                        ArrayList<String> data_type = new ArrayList<String>();
                        ArrayList<String> nullable = new ArrayList<String>();
                        readFromDavisbase_columns(tablename, col_names, data_type, nullable);
                        
//                        
                        Boolean validate = validateInsert(col_names,data_type,nullable,insertTableString,primary_values);
                        if(validate)
                        {

                            String cols = insertTableString.substring(insertTableString.indexOf("(")+1,insertTableString.indexOf(")"));
                            String[] columns = cols.trim().split(",");
                            String vals = insertTableString.substring(insertTableString.indexOf(")")+1);
                       
                            String values = vals.substring(vals.indexOf("(")+1,vals.indexOf(")"));
                            String[] ind_values = values.trim().split(",");
                            int recordSize = 0;
                            for (int i = 0; i < data_type.size(); i++)
                            {
                                if(i>= ind_values.length)
                                {
                                    
                                    if(data_type.get(i).equals("text"))
                                    {
                                        recordSize+= "null".length()+1;
                                    }
                                    else
                                    {
                                        recordSize += getNoBytes(data_type.get(i));
                                    }
                                }
                                else{
                                    if(data_type.get(i).equals("text"))
                                    {
                                        recordSize+= ind_values[i].replace("\""," ").trim().length()+1;
                                    }
                                    else
                                    {
                                        recordSize += getNoBytes(data_type.get(i));
                                    }
                                }
                              
                            }
                   
                            try {
                                    RandomAccessFile tableFile = new RandomAccessFile("data/user_data/"+tablename+"/"+tableFileName, "rw");
                                    int record_location;
                                    int pos;
                                    if(!last_primarykey.containsKey(tablename))
                                    {
                                        
                                        
                                        record_location = (int) (tableFile.length() - recordSize);
                                        
                                        
                                   }
                                    else
                                    {
                                        pos =Davisbase_table_trees.get(tablename).searchKey(last_primarykey.get(tablename));
                                        record_location = pos - recordSize;
                                    }
                                    int no_pages = (int) (tableFile.length()/512);
                                    if(record_location < (((no_pages-1)*512)+8))
                                    {
                                         tableFile.setLength(tableFile.length()+pageSize);
                                         pos = (int) (tableFile.length()+ 512);
                                    }
                                   
                                    tableFile.seek(record_location);
                                    for (int i = 0; i < col_names.size(); i++)
                                        {
                                            
                                            if(i>= ind_values.length)
                                            {

                                               if(data_type.get(i).equals("text"))
                                               {
                                                   tableFile.writeByte(4);
                                                   tableFile.writeBytes("null");
                                               }
                                               else if(data_type.get(i).equals("int"))
                                               {
                                                   tableFile.writeInt(-1);
                                               }
                                               else if(data_type.get(i).equals("tinyint"))
                                               {
                                                   tableFile.writeByte(-1);
                                               }
                                               else if(data_type.get(i).equals("smallint"))
                                               {
                                                   tableFile.writeShort(-1);
                                               }
                                               else if(data_type.get(i).equals("bigint"))
                                               {
                                                   tableFile.writeLong(-1);
                                               }
                                               else if(data_type.get(i).equals("real"))
                                               {
                                                   tableFile.writeFloat(-1);
                                               }
                                               else if(data_type.get(i).equals("double"))
                                               {
                                                   tableFile.writeDouble(-1);
                                               }
                                               else if(data_type.get(i).equals("datatime") || data_type.get(i).equals("date"))
                                               {
                                                   ZoneId zoneId = ZoneId.of ( "America/Chicago" );

                                                   /* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
                                                   ZonedDateTime zdt = ZonedDateTime.of (1974,5,27,0,0,0,0, zoneId );

                                                   /* ZonedDateTime toLocalDate() method will display in a simple format */
                                                    

                                                   /* Convert a ZonedDateTime object to epochSeconds
                                                    * This value can be store 8-byte integer to a binary
                                                    * file using RandomAccessFile writeLong()
                                                    */
                                                   long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
                                                   tableFile.writeLong( -1 );
                                               }
                                            }
                                            else if(data_type.get(i).equals("text"))
                                            {
                                                tableFile.writeByte(ind_values[i].replace("\""," ").trim().length());
                                                tableFile.writeBytes(ind_values[i].replace("\""," ").trim());
                                            }
                                            else if(data_type.get(i).equals("int"))
                                            {
                                                tableFile.writeInt(Integer.parseInt(ind_values[i]));
                                            }
                                            else if(data_type.get(i).equals("tinyint"))
                                            {
                                                tableFile.writeByte(Integer.parseInt(ind_values[i]));
                                            }
                                            else if(data_type.get(i).equals("smallint"))
                                            {
                                                tableFile.writeShort(Short.parseShort(ind_values[i]));
                                            }
                                            else if(data_type.get(i).equals("bigint"))
                                            {
                                                tableFile.writeLong(Long.parseLong(ind_values[i]));
                                            }
                                            else if(data_type.get(i).equals("real"))
                                            {
                                                tableFile.writeFloat(Float.parseFloat(ind_values[i]));
                                            }
                                            else if(data_type.get(i).equals("double"))
                                            {
                                                tableFile.writeDouble(Double.parseDouble(ind_values[i]));
                                            }
                                            else if(data_type.get(i).equals("datatime") || data_type.get(i).equals("date"))
                                            {
                                                ZoneId zoneId = ZoneId.of ( "America/Chicago" );

                                                /* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
                                                ZonedDateTime zdt = ZonedDateTime.of (1974,5,27,0,0,0,0, zoneId );

                                                /* ZonedDateTime toLocalDate() method will display in a simple format */
                                                

                                                /* Convert a ZonedDateTime object to epochSeconds
                                                 * This value can be store 8-byte integer to a binary
                                                 * file using RandomAccessFile writeLong()
                                                 */
                                                long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
                                                tableFile.writeLong( epochSeconds );
                                            }
                                        }   
                                        last_primarykey.put(tablename, Integer.parseInt(ind_values[0]));
                                        Davisbase_table_trees.get(tablename).insertKey(Integer.parseInt(ind_values[0]), record_location);
                                    //code to write to ndx files:
                                    for( int x = 0; x <col_names.size(); x++)
                                    {
                                        File f = new File("data/user_data/"+tablename+"/"+col_names.get(x)+".ndx");
                                        RandomAccessFile ndx = new RandomAccessFile(f, "rw");
                                        ndx.seek(ndx.length());
                                            if(x >= ind_values.length)
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeBytes("null\n");
                                            }
                                            else if(data_type.get(x).equals("text"))
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeBytes(ind_values[x].replace("\""," ").trim()+"\n");
                                            }
                                            else if(data_type.get(x).equals("int"))
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0])); 
                                                ndx.writeInt(Integer.parseInt(ind_values[x]));
                                            }
                                            else if(data_type.get(x).equals("tinyint"))
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeByte(Integer.parseInt(ind_values[x]));
                                            }
                                            else if(data_type.get(x).equals("smallint"))
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeShort(Short.parseShort(ind_values[x]));
                                            }
                                            else if(data_type.get(x).equals("bigint"))
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeLong(Long.parseLong(ind_values[x]));
                                            }
                                            else if(data_type.get(x).equals("real"))
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeFloat(Float.parseFloat(ind_values[x]));
                                            }
                                            else if(data_type.get(x).equals("double"))
                                            {
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeDouble(Double.parseDouble(ind_values[x]));
                                            }
                                            else if(data_type.get(x).equals("datatime") || data_type.get(x).equals("date"))
                                            {
                                                ZoneId zoneId = ZoneId.of ( "America/Chicago" );

                                                /* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
                                                ZonedDateTime zdt = ZonedDateTime.of (1974,5,27,0,0,0,0, zoneId );

                                                 

                                                /* Convert a ZonedDateTime object to epochSeconds
                                                 * This value can be store 8-byte integer to a binary
                                                 * file using RandomAccessFile writeLong()
                                                 */
                                                long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
                                                ndx.writeInt(Integer.parseInt(ind_values[0]));
                                                ndx.writeLong( epochSeconds );
                                            }
                                        
                                    }
                                } catch (FileNotFoundException ex) {
                                    Logger.getLogger(Davisbase.class.getName()).log(Level.SEVERE, null, ex);
                                }


                        }
                        else
                        {
                            System.out.println("Validation error check syntax again and retype");
                        }
                }
                }
                catch(Exception e)
                {
                    
                }
                
        }                
          
         public void parseShowTables(String usrcmd)
        {
            if (usrcmd.equalsIgnoreCase("show tables"))
            {
               try
            {
                 RandomAccessFile obj = new RandomAccessFile("data//catalog//davisbase_tables.tbl","rw"); 

                 System.out.println("row_id"+"      "+"table");
                 System.out.println("-------------------------------------------");
                 for (int i = 1 ; i<= last_primarykey.get("davisbase_tables") ; i++ )
                 {
                     try
                     {
                         
                         int position = Davisbase_table_trees.get("davisbase_tables").searchKey(i);
                         obj.seek(position);
                         int row_id = obj.readInt();
                         
                         int size = obj.readByte();
                         byte[] str = new byte[size]; 
                         obj.read(str);
                         System.out.println(row_id+"        "+new String(str));

                     }
                     catch (Exception e)
                     {
                         
                     }
            }
        }
               catch (Exception e)
                     {
                         
                     }
            }
            
        }

       public void parseDeleteString(String deleteString)
       {
           ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(deleteString.split(" ")));
           String tablename = deleteTableTokens.get(3);
           if( tablename.equals("davisbase_tables")||tablename.equals("davisbase_columns"))
           {
               System.out.println("Cannot delete from Davisbase_tables and Davisbase_columns ");
           }
           else{
           int rowid = Integer.parseInt(deleteTableTokens.get(7));
           ArrayList<String> col_names = new ArrayList<String>();
           ArrayList<String> data_type = new ArrayList<String>();
           ArrayList<String> nullable = new ArrayList<String>();
           readFromDavisbase_columns(tablename, col_names, data_type, nullable);
           try{
               RandomAccessFile tableFile;

                    tableFile = new RandomAccessFile("data/user_data/"+tablename+"/"+tablename+".tbl","rw");
                
                int location = Davisbase_table_trees.get(tablename).searchKey(rowid);
                tableFile.seek(location);
                int count =0;
                for(int j =0 ; j < col_names.size();j++)
                                    {
                                        if(data_type.get(j).equals("tinyint"))
                                        {
                                            count +=1;
                                           
                                        }
                                        else if(data_type.get(j).equals("smallint"))
                                        {
                                            count+=2;
                                                                                    
                                        }
                                        else if(data_type.get(j).equals("int"))
                                        {
                                           count+=4;
                                        }
                                        else if(data_type.get(j).equals("bigint"))
                                        {
                                            count +=8;
                                                                               }
                                        else if(data_type.get(j).equals("real"))
                                        {
                                            count +=4;
                                        }                                        
                                        else if(data_type.get(j).equals("double"))
                                        {
                                            count+=8;
                                        }
                                        else if(data_type.get(j).equals("date")||data_type.get(j).equals("datetime"))
                                        {
                                            count+=8;
                                            
                                        }
                                        else if(data_type.get(j).equals("text"))
                                        {
                                            tableFile.seek(location + count);
                                            int size = tableFile.readByte();
                                            count+=size;
                                         }
                                    }
                tableFile.seek(location);
                for(int x =0; x <= count; x++)
                {
                    tableFile.writeByte(0);
                }
           }
           
           catch(Exception e)
           {
               
           }
           Davisbase_table_trees.get(tablename).removeKey(rowid);
       
           }
       }
       
    public void parseDropTables(String usercmd)
    {   
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(usercmd.split(" ")));
        
        String tablename = createTableTokens.get(2).trim();
        String tableFile = createTableTokens.get(2)+".tbl";
        boolean flag = false;
        try
        {
            RandomAccessFile davisbase_table = new RandomAccessFile("data/catalog/davisbase_tables.tbl","rw");
            
             //DElete from Tables.
            for (int i = 0; i< last_primarykey.get("davisbase_tables") ; i++ )
            {
                try
                {
                    int location_tab = Davisbase_table_trees.get("davisbase_tables").searchKey(i+1);
                    davisbase_table.seek(location_tab);
                    davisbase_table.readInt();
                    int size = davisbase_table.readByte();
                    byte[] str = new byte[size];
                    davisbase_table.read(str);
                    String strr = new String(str);
                    if(tablename.equalsIgnoreCase(strr))
                    {
                        int recordsize = 4+1+size;
                        davisbase_table.seek(location_tab);
                        for(int j=0; j < recordsize; j++)
                            davisbase_table.writeByte(0);
                        flag = true;
                        Davisbase_table_trees.get("davisbase_tables").removeKey(i+1);   
                    }
                }
                catch(Exception ex)
                {
                    System.out.println(ex+"here");
                }
                       
                
            }
        }
        catch(Exception e)
        {
        }
            //Delete from Columns.
            boolean rec = false;
          RandomAccessFile davisbase_columns;
            try {
                davisbase_columns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                int Size;
                        for(int i =1; i<=last_primarykey.get("davisbase_columns");i++)
                        {
                            try{
                                int pos = Davisbase_table_trees.get("davisbase_columns").searchKey(i);
                                davisbase_columns.seek(pos);
                                int rowid = davisbase_columns.readInt();
                                int tablename_size = davisbase_columns.readByte();
                                byte[] s = new byte[tablename_size];
                                davisbase_columns.read(s);
                                String tablenm = new String(s);
                                Size= 5;
                                if(tablenm.equals(tablename))
                                {           
                                    Size +=tablename_size;
                                    int x= davisbase_columns.readByte(); 
                                    Size +=x+1;
                                    davisbase_columns.seek(davisbase_columns.getFilePointer()+x);
                                    int y= davisbase_columns.readByte(); 
                                    Size +=y+1;
                                    davisbase_columns.seek(davisbase_columns.getFilePointer()+y);
                                    Size +=1;
                                    davisbase_columns.seek(davisbase_columns.getFilePointer()+1);
                                    int z= davisbase_columns.readByte();
                                    Size +=z+1;
                                    rec = true;
                                   Davisbase_table_trees.get("davisbase_columns").removeKey(i); 
                                }
                            
                            if (rec)
                            {
                                davisbase_columns.seek(pos);
                                for (int j = 0; j < Size ; j ++)
                                {
                                     davisbase_columns.writeByte(0);
                                }           
                }

            }
                catch(Exception ex)
        {
            System.out.println(ex);
        }            
        }
           if(flag)
           {
               try{
                   
                    File file = new File("data/user_data/"+tablename);
                    String[] s = file.list();
                    for(String str: s)
                    {
                        File currentFile = new File(file.getPath(),str);
                        currentFile.delete();
                    }
                    file.delete();
                }
       catch(Exception ex)
       {
           System.out.println(ex);
       }   
           }
           else
           {
               System.out.println("Table not found");
         
           }
    
    }
      catch(Exception ex)
       {
           System.out.println(ex);
       }   
    Davisbase_table_trees.remove(tablename);
    last_primarykey.remove(tablename);
    }

    
       
       public Boolean validateInsert(ArrayList<String> col_names,ArrayList<String> data_type,ArrayList<String> nullable,String usercmd, ArrayList<Integer> primary_keys)
       {
           String cols = usercmd.substring(usercmd.indexOf("(")+1,usercmd.indexOf(")"));
           String[] columns = cols.trim().split(",");
           String vals = usercmd.substring(usercmd.indexOf(")")+1);
          String values = vals.substring(vals.indexOf("(")+1,vals.indexOf(")"));
          String[] ind_values = values.trim().split(",");
          if(ind_values.length<=0)
          {
              System.out.println("No values to insert");
              return false;
          }
          if(columns.length > col_names.size() )
          {
             System.out.println("Wrong number of columns");
              return false;
          }
          if(primary_keys.size()>0 && primary_keys.contains(Integer.parseInt(ind_values[0])))
          {
              System.out.println("Primary Key Constraint violated");
              return false;
          }
          for (int i=0; i< columns.length;i++)
          { 
            if(!columns[i].trim().equalsIgnoreCase(col_names.get(i)))
            {
                System.out.println("Columns are in wrong order");
                return false;
            }
          }
          int k = col_names.size() - columns.length;
          if (k !=0)
          {
              for (int j=k ; j < col_names.size() ; j++)
              {
                  if(nullable.get(j).equals("no"))
                  {
                      System.out.println("Not null attibute not provided");
                      return false;
                  }
              }//insert into emp (ssn, name) values (1, "namee"); 
              //create table emp (ssn int primary key, name text);
              // 
          }
                    
           return true;
       }
       public boolean validateCreateString(String s)
        {
            ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(s.split(" ")));
            
            String table_str = createTableTokens.get(1);
            String tableName = createTableTokens.get(2);
            String[] contents = s.substring(s.indexOf("(")+1,s.indexOf(")")).split(",");
            if ( !table_str.matches("table"))
            {
                
                return false;
            }
           else if (Davisbase_table_trees.containsKey(tableName)) 
            {
                return false;   
            }
           else if(!contents[0].contains("int primary key"))
                    {
                        return false;
                    }  
           else if(!tableName.matches("[a-zA-Z]+"))
                   {
                       return false;
                   }
              
            return true;
        }
        public void meta_tables(){
            try{

                Davisbase_table_trees.put("davisbase_tables", new BPlusTree());
                Davisbase_table_trees.put("davisbase_columns", new BPlusTree());
                RandomAccessFile tableFile1 = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

                tableFile1.setLength(pageSize);
                writePageHeader(tableFile1);
                RandomAccessFile tableFile2 = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
		tableFile2.setLength(pageSize);
                writePageHeader(tableFile1);
                int recordSize =0;
                int noRecords = Davisbase_table_trees.get("davisbase_tables").No_Nodes()+ 1;
                recordSize += 4 + "davisbase_tables".length()+1;
                int recordLocation = pageSize - (recordSize);
                int currentPage = 0;
		int pageLocation = pageSize * currentPage;
                int val = pageLocation + recordLocation;
                last_primarykey.put("davisbase_tables",1);
                Davisbase_table_trees.get("davisbase_tables").insertKey(1,val);
                tableFile1.seek(pageLocation + recordLocation);
                tableFile1.writeInt(1);                           // Write rowid=1
                tableFile1.writeByte("davisbase_tables".length());
		tableFile1.writeBytes("davisbase_tables");
                updatePageHeader(tableFile1, recordLocation, Davisbase_table_trees.get("davisbase_tables").No_Nodes());
                tableFile1.close();
                tableFile1 = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
                noRecords = Davisbase_table_trees.get("davisbase_tables").No_Nodes()+1;
                int rc= 1 +  4 + "davisbase_columns".length() ;
                recordSize += 1+ 4 + "davisbase_columns".length();
                recordLocation = pageSize - (recordSize);
                val = pageLocation + recordLocation;
                last_primarykey.put("davisbase_tables",2);
                Davisbase_table_trees.get("davisbase_tables").insertKey(2,val);
                tableFile1.seek(pageLocation + recordLocation); /* Locate the pointer to the beginning of the page */
                tableFile1.writeInt(2);  
                tableFile1.writeByte("davisbase_columns".length());// Write rowid=1
		tableFile1.writeBytes("davisbase_columns");
                updatePageHeader(tableFile1, recordLocation, Davisbase_table_trees.get("davisbase_tables").No_Nodes());
                tableFile1.close();
                /*inserting into davisbase_columns */
                String[] table_name = {"davisbase_tables","davisbase_tables","davisbase_columns","davisbase_columns","davisbase_columns","davisbase_columns","davisbase_columns","davisbase_columns"};
                String[] column_name = {"rowid","table_name","rowid","table_name","column_name","data_type","ordinal_position","is_nullable"};
                String[] data_type = {"int","text","int","text","text","text","tinyint","text"};
                Byte[] ordinal_position = {1,2,1,2,3,4,5,6};
                String[] is_nullable = {"no","no","no","no","no","no","no","no"};
                currentPage = 0;
		pageLocation = pageSize * currentPage;
                recordSize = 0;
                
               for(int i =0; i <8; i++)
                {
                    
                    tableFile2 = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                    noRecords = i+1;
                    rc = 4 + table_name[i].length() + column_name[i].length() + data_type[i].length() + 1+ is_nullable[i].length()+4;
                    recordSize += rc;
                    recordLocation = pageSize - (recordSize);
                    val = pageLocation + recordLocation;
                    if(val > (8+((noRecords)*2)))
                    {
                        last_primarykey.put("davisbase_columns",noRecords);
                        Davisbase_table_trees.get("davisbase_columns").insertKey(noRecords,pageLocation + recordLocation);
                        tableFile2.seek(pageLocation + recordLocation); 
                        
                        tableFile2.writeInt(noRecords);
                        tableFile2.writeByte(table_name[i].length());
                        tableFile2.writeBytes(table_name[i]);
                        tableFile2.writeByte(column_name[i].length());
                        tableFile2.writeBytes(column_name[i]);
                        tableFile2.writeByte(data_type[i].length());
                        tableFile2.writeBytes(data_type[i]);
                        tableFile2.writeByte(ordinal_position[i]);
                        tableFile2.writeByte(is_nullable[i].length());
                        tableFile2.writeBytes(is_nullable[i]);
                        
                    }
                    
                    tableFile2.close();
                }

            }
            catch(Exception e){
                
            }
        }
    static void displayBinaryHex(RandomAccessFile raf) {
		try {
			System.out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
			raf.seek(0);
			long size = raf.length();
			int row = 1;
			System.out.print("0000\t0x0000\t");
			while(raf.getFilePointer() < size) {
				System.out.print(String.format("%02X ", raf.readByte()));
				if(row % 16 == 0) {
					System.out.println();
					System.out.print(String.format("%04d\t0x%04X\t", row, row));
				}
				row++;
			}
		}
		catch (Exception e) {
			
		}
	}
    public void data_directories(){
        File dir = new File("data");
        dir.mkdir();
        File dir1 = new File("data\\catalog");
        dir1.mkdir();
        File dir2 = new File("data\\user_data");
        dir2.mkdir();
        
    }
    
    public HashMap<Integer, String[]> parseColumnNames(String userCommand)
    {
          HashMap<Integer, String[]> col = new HashMap<Integer,String[]>();
          String[] vals = userCommand.substring(userCommand.indexOf("(")+ 1,userCommand.indexOf(")")).trim().split(",");
          col.put(1, new String[] {vals[0].trim().split(" ")[0],vals[0].trim().split(" ")[1],"1"});
          for (int i =1; i < vals.length; i++)
          {
              String[] attribute = vals[i].trim().split(" ");
              if(attribute.length == 2)
              {
                  col.put(i+1, new String[] {attribute[0],attribute[1],"0"});
              }
              else 
              {
               
                 col.put(i+1,new String[] {attribute[0],attribute[1],"1"});
              }                                               
          }
          return col;
    }
    
    public void writeToDavisbase_tables(String tablename)
    {
        Davisbase_table_trees.put(tablename, new BPlusTree());
            try {
                RandomAccessFile tableFile = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
                int lastrecord = last_primarykey.get("davisbase_tables");
                int lastrecordLocation = Davisbase_table_trees.get("davisbase_tables").searchKey(lastrecord);
                
                int record_size = 1+ 4 +tablename.length();
                int no_pages = (int) (tableFile.length()/512);
                int recordLocation = lastrecordLocation - record_size;
                if(recordLocation < (((no_pages-1)*512)+8))
                    {
                        tableFile.setLength(tableFile.length()+pageSize);
                        lastrecordLocation = (int) (tableFile.length()+ 512);
                    }
                tableFile.seek(lastrecordLocation - record_size);
                Davisbase_table_trees.get("davisbase_tables").insertKey(lastrecord+1,lastrecordLocation - record_size);
                tableFile.writeInt(lastrecord+1);
                tableFile.writeByte(tablename.length());
                tableFile.writeBytes(tablename);
                tableFile.close();
                last_primarykey.put("davisbase_tables",lastrecord+1);
            } catch (Exception ex) {
                
            }
        
    }
    public void writeToDavisbase_columns(String table_name,String[] column_names, String[] datatypes, String[] is_null)
    {
        try {
            RandomAccessFile tableFile;
            int lastrecord ; 
            int lastrecordLocation ;
            int recordSize = 0;
            int recordLocation;
            for(int i =0; i < column_names.length; i++)
                {
                    
                    lastrecord = last_primarykey.get("davisbase_columns");
                    lastrecordLocation = Davisbase_table_trees.get("davisbase_columns").searchKey(lastrecord);
                    tableFile = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                    if (is_null[i] == "0")
                    {
                        recordSize = 4 + 4 + table_name.trim().length() + column_names[i].trim().length() + datatypes[i].trim().length()+ 1 + 3;
                    }
                    else
                    {
                        recordSize = 4 + 4 + table_name.trim().length() + column_names[i].trim().length() + datatypes[i].trim().length()+ 1 + 2;
                    }
                    recordLocation = lastrecordLocation - (recordSize);
                    
                    int no_pages = (int) (tableFile.length()/512);
                    if(recordLocation < (((no_pages-1)*512)+8))
                    {
                        tableFile.setLength(tableFile.length()+pageSize);
                        lastrecordLocation = (int) (tableFile.length()+ 512);
                    }
                    Davisbase_table_trees.get("davisbase_columns").insertKey(lastrecord+1,recordLocation);
                    tableFile.seek(recordLocation); /* Locate the pointer to the beginning of the page */
                    tableFile.writeInt(lastrecord+1);
                    tableFile.writeByte(table_name.length());
                    tableFile.writeBytes(table_name.trim());
                    tableFile.writeByte(column_names[i].length());
                    tableFile.writeBytes(column_names[i].trim());
                    tableFile.writeByte(datatypes[i].length());
                    tableFile.writeBytes(datatypes[i].trim());
                    tableFile.writeByte(i+1);
                    if(is_null[i] == "0")
                    {
                        tableFile.writeByte("yes".length());
                        tableFile.writeBytes("yes");
                    }
                    else
                    {
                        tableFile.writeByte("no".length());
                        tableFile.writeBytes("no");
                    }
                    last_primarykey.put("davisbase_columns",lastrecord+1);
                    tableFile.close();
                    }
                }
            
            
         catch (Exception e) {
            
        }
    }
    public void readFromDavisbase_columns(String tablename, ArrayList<String> col_names, ArrayList<String> data_type, ArrayList<String> nullable )
    {
        RandomAccessFile davisbase_columns;
            try {
                davisbase_columns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                        for(int i =1; i<=last_primarykey.get("davisbase_columns");i++)
                        {
                            try{
                                int pos = Davisbase_table_trees.get("davisbase_columns").searchKey(i);
                                davisbase_columns.seek(pos);
                                int rowid = davisbase_columns.readInt();
                                int tablename_size = davisbase_columns.readByte();
                                byte[] s = new byte[tablename_size];
                                davisbase_columns.read(s);
                                String tablenm = new String(s);
                                if(tablenm.equals(tablename))
                                {
                                    
                                    int col_size = davisbase_columns.readByte();
                                    byte[] c = new byte[col_size];
                                    davisbase_columns.read(c);
                                    col_names.add(new String(c));
                                    int datatype_size = davisbase_columns.readByte();
                                    byte[] d = new byte[datatype_size];
                                    davisbase_columns.read(d);
                                    data_type.add(new String(d));
                                    int ordinal_position = davisbase_columns.readByte();
                                    int null_size = davisbase_columns.readByte();
                                    byte[] n = new byte[null_size];
                                    davisbase_columns.read(n);
                                    nullable.add(new String(n));
                                }
                            }
                            catch(NullPointerException e)
                            {
                                
                            } catch (IOException ex) {
                        
                    }
                            
                        }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Davisbase.class.getName()).log(Level.SEVERE, null, ex);
            }
    }
    public void ndxfiles(String tablename,String[] column_names)
    {
        try {
            for(int i =0; i < column_names.length; i++)
            {
                if(tablename.equals("davisbase_tables")|| tablename.equals("davisbase_columns"))
                {
                    RandomAccessFile tableFile = new RandomAccessFile("data/catalog/"+column_names[i]+".ndx", "rw");
                }
                else
                {
                    RandomAccessFile tableFile = new RandomAccessFile("data/user_data/"+tablename+"/"+column_names[i]+".ndx", "rw");
                }
                
                
            }
            
            
        } catch (Exception e) {
        }
    }
    
    public int getNoBytes(String datatype)
    {
        int size = 0;
        if(datatype.equals("tinyint"))
        {
           size = 1;
        }
        else if(datatype.equals("smallint"))
        {
            size = 2;
        }
        else if(datatype.equals("int") || datatype.equals("real"))
        {
            size = 4;
        }
        else if(datatype.equals("bigint") || datatype.equals("double") || datatype.equals("date") || datatype.equals("datetime"))
        {
            size = 8;
        }
              
        return size;
    }

    public void saveState()
    {
        File dir = new File("state");
        if (!dir.exists()) 
        {
            dir.mkdir();
            File t = new File("state//trees");
            t.mkdir();
        }
        Set<String> trees = Davisbase_table_trees.keySet();
        for(String name : trees)
        {
            try {
                FileWriter writer = new FileWriter("state/trees/"+name+".txt",true);
            
            String s="";
            try{
                for(int i =0; i<last_primarykey.get(name);i++)
                {
                    
                    s+=(i+1)+",";
                    s+=Davisbase_table_trees.get(name).searchKey(i+1)+"\n";
                    writer.write(s);
                    
                }
            }
            catch(Exception e)
            {
                
            }
            writer.close();
           } 
            catch (IOException ex) 
            {
               
            } 
        }
        try{
            FileWriter writer = new FileWriter("state/last_key.txt");
            for(String key : last_primarykey.keySet())
            {
                writer.write(key+","+last_primarykey.get(key)+"\n");
            }
            writer.close(); 
        }
        catch(Exception e){
            
        }
       
    }
    public void getState()
    {
        File dir = new File("data");
        if (!dir.exists()) 
        {
            data_directories();
            meta_tables();
        }
        else
        {
            File folder = new File("state/trees");
            File[] listOfFiles = folder.listFiles();

            for (int i = 0; i < listOfFiles.length; i++) 
            {
                if (listOfFiles[i].isFile()) {
                Davisbase_table_trees.put(listOfFiles[i].getName().substring(0,listOfFiles[i].getName().indexOf(".txt")),new BPlusTree());
                } 
            }
            for( int i=0; i < listOfFiles.length; i++)
            {
                try {
                   
                    FileReader reader = new FileReader("state/trees/"+listOfFiles[i].getName());
                    BufferedReader bufferedReader = new BufferedReader(reader);
                    String line;
                     while((line = bufferedReader.readLine()) != null) {
                        Davisbase_table_trees.get(listOfFiles[i].getName().substring(0,listOfFiles[i].getName().indexOf(".txt"))).insertKey(Integer.parseInt(line.split(",")[0]),Integer.parseInt(line.split(",")[1]));
                    }   
                    
                    } catch (FileNotFoundException ex) {
                    Logger.getLogger(Davisbase.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(Davisbase.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            
            try {
                    FileReader reader = new FileReader("state/last_key.txt");
                    BufferedReader bufferedReader = new BufferedReader(reader);
                    String line;
                     while((line = bufferedReader.readLine()) != null) {
                    
                    last_primarykey.put(line.split(",")[0],Integer.parseInt(line.split(",")[1]));
                     }
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(Davisbase.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                Logger.getLogger(Davisbase.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        
    }
    public void writePageHeader(RandomAccessFile f)
    {
        try{
            f.writeByte(13);
            f.writeByte(0);
            f.writeShort(pageSize);
            f.writeInt(0);
        }
        catch(Exception e)
        {
            
        }
    }
    public void updatePageHeader(RandomAccessFile f,int loc, int n)
    {
            try {
                f.seek(1);
                f.writeByte(n);
                f.seek(8+ ((n-1)*2));
                f.writeShort(loc);
            } catch (Exception e) {
               
            }
        
    }
}
    
class BPlusTree {
        TreeMap<Integer,Integer> tree = new TreeMap<Integer,Integer>();
	public void insertKey(int key, int value) {
		this.tree.put(key, value);
	}
	
	public void removeKey(int key) {
		this.tree.remove(key);
	}
        
        public int searchKey(int key)
        {
           
                return this.tree.get(key);
           
        }
        public int No_Nodes()
        {
            return this.tree.size();
        }

    }
