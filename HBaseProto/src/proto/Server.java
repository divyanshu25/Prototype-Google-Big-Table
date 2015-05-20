package proto;

import java.util.HashMap;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Server {

	private int replicas=10;
	private int MOD=1000;
	private TreeMap<Integer,Integer> ring=new TreeMap<Integer,Integer>();
	private HashMap<Integer,String> ipMapping=new HashMap<Integer, String>();
	private Socket[] tabletServer;
	private int noOfTabletServers;
	private DataOutputStream[] out;
	private DataInputStream[] in;
	private int PORT;
	private HashMap<String,String> tableList=new HashMap<String, String>();
	private HashMap<String,String> SchemaTableMap = new HashMap<String,String>();
	private int[] isNodeAlive;

	public Server(int port,int numServers) {
		PORT=port;
		noOfTabletServers=numServers;
		tabletServer =new Socket[noOfTabletServers];
		out=new DataOutputStream[noOfTabletServers];
		in=new DataInputStream[noOfTabletServers];
		isNodeAlive=new int[noOfTabletServers];
		
		try {
			Scanner scanner=new Scanner(new FileInputStream("tablenames.txt"));
			
			int num=scanner.nextInt();
			scanner.nextLine();
			
			for(int i=0;i<num;i++){
				String str=scanner.nextLine();
				tableList.put(str,str);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void addNode(int node,String ip){

		MessageDigest messageDigest;
		isNodeAlive[node]=1;
		ipMapping.put(node,ip);
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
			for (int r = 0; r < replicas; r++) {
				messageDigest.update( ("TabletServer"+node+String.valueOf(r*r*r)).getBytes());
				String encryptedString = new String(messageDigest.digest());
				int hash = Math.abs(encryptedString.hashCode())%MOD;
				ring.put(hash,node);
				System.out.println(hash);
			}

			try{
				System.out.println(node+".............."+ipMapping.get(node));
				tabletServer[node]=new Socket(ipMapping.get(node),PORT);

				OutputStream outToServer = tabletServer[node].getOutputStream();
				out[node] = new DataOutputStream(outToServer);

				InputStream inFromServer = tabletServer[node].getInputStream();
				in[node] =new DataInputStream(inFromServer);
			} catch(IOException e){
				e.printStackTrace();
			}

		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private int getNode(String data)
	{

		if(ring.size()==0){
			System.out.println("Empty ring");
			return -1;
		}

		MessageDigest messageDigest;
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
			messageDigest.update(data.getBytes());
			String encryptedString = new String(messageDigest.digest());
			int hash = Math.abs(encryptedString.hashCode())%MOD;

			SortedMap<Integer, Integer> tail = ring.tailMap(hash);

			int node=0;
			if (!tail.isEmpty())
			{
				node=tail.firstKey();
				node=ring.get(node);
			}

			if(isNodeAlive[node]==1)
				return node;
			else
				return (node+1)%noOfTabletServers;
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return -1;
	}

	private void createTable(String tableName){
		for(int i=0;i<noOfTabletServers;i++){
			try {
				out[i].writeUTF(0+" "+tableName);
			} catch(EOFException eof){
				isNodeAlive[i]=0;
			}

			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void insertIntoTable(String tableName,String key,String value){
		int node=getNode(key);
		int first=node;
		int second=(node+1)%noOfTabletServers;
		System.out.println("first= "+first+", second= "+second);
		try {
			out[first].writeUTF(1+" "+tableName+" "+key+" "+value);
			out[second].writeUTF(1+" "+tableName+" "+key+" "+value);
		} catch(EOFException eof){
			isNodeAlive[first]=0;
			try {
				out[second].writeUTF(1+" "+tableName+" "+key+" "+value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void deleteFromTable(String tableName,String key){
		int node=getNode(key);
		try {
			out[node].writeUTF(2+" "+tableName+" "+key);
		} catch(EOFException eof){
			isNodeAlive[node]=0;
		}  
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String getData (String tableName,String key){
		int node=getNode(key);
		String value="";
		try {
			out[node].writeUTF(3+" "+tableName+" "+key);
			value=in[node].readUTF();
		} catch(EOFException eof){
			isNodeAlive[node]=0;
			node=(node+1)%noOfTabletServers;
			try {
				out[node].writeUTF(3+" "+tableName+" "+key);
				value=in[node].readUTF();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return value;
	}
	public static void main(String args[]){

		try {
			Scanner scanner=new Scanner(new FileInputStream("tablet-servers.txt"));

			int numTabletServers=scanner.nextInt();
			scanner.nextLine();
			Server server=new Server(2400,numTabletServers);

			for (int i=0;i<numTabletServers;i++){
				String str=scanner.nextLine();
				System.out.println("str........."+str);
				server.addNode(i,str);
			}

			Scanner in = new Scanner(System.in);
			int choice;
			System.out.println("Welcme to BIG TABLE");

			while(true){
				System.out.println("\n1. Create New Table");
				System.out.println("2. Add Data to a Table");
				System.out.println("3. Find Data by Key");
				System.out.println("4. Delete Data by Key");
				System.out.println("Enter your Choice:");
				choice = in.nextInt();
				in.nextLine();
				if(choice == 1){
					System.out.print("Enter Schema File Path for New Table: ");
					String filename = in.nextLine();
					//in.nextLine();
					Scanner scanner1=new Scanner(new FileInputStream("Schemas/"+filename));
					String tableName = scanner1.nextLine();
					if(server.SchemaTableMap.containsKey(tableName)){
						System.out.println("Table with same name already exist");
					}
					else{
						server.SchemaTableMap.put(tableName, filename);
						server.createTable(tableName);
						server.tableList.put(tableName,tableName);
					}

				}
				if(choice == 2){
					System.out.println("Enter table name:");
					String tableName=in.nextLine();

					if(server.tableList.containsKey(tableName)){
						System.out.print("Enter File Name:");
						String fileName = in.nextLine();
						System.out.println(fileName);
						Scanner scanner1=new Scanner(new FileInputStream(fileName));

						while(scanner1.hasNextLine()){
							String str=scanner1.nextLine();
							System.out.println(str);
							String[] toks=str.split(" ");
							server.insertIntoTable(tableName,toks[0], toks[1]);
						}
					}
					else{
						System.out.println("Table doesn't exist");
					}

				}
				if(choice == 3){
					System.out.print("Enter table name: ");
					String tableName = in.nextLine();
					if(server.tableList.containsKey(tableName)){
						System.out.print("Enter key to search: ");
						String key = in.nextLine();
						String value=server.getData(tableName,key);
						System.out.println("The value is "+ value);
					}

					else{
						System.out.println("Table doesn't exist");
					}
				}
				if(choice == 4){
					System.out.print("Enter table name: ");
					String tableName = in.nextLine();

					if(server.tableList.containsKey(tableName)){
						System.out.print("Enter key Delete: ");
						String key = in.nextLine();
						server.deleteFromTable(tableName, key);
					}

					else{
						System.out.println("Table doesn't exist");
					}
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("File not found");
		}
		//server.addNode(1,"localhost");
		//server.addNode(2,"localhost");

		/*System.out.println(getNode("Hahah"));
		System.out.println(getNode("Rose"));
		System.out.println(getNode("Jolly"));
		System.out.println(getNode("Hahah"));
		System.out.println(getNode("Hermoine"));
		System.out.println(getNode("Weasly"));
		System.out.println(getNode("Harry"));*/
	}
}
