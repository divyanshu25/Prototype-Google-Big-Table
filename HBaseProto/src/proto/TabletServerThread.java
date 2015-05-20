package proto;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TabletServerThread implements Runnable {

	private DataInputStream in = null;
	private DataOutputStream out = null;

	public TabletServerThread(Socket s) {

		try {
			InputStream inSocket = s.getInputStream();
			in=new DataInputStream(inSocket);

			OutputStream outSocket = s.getOutputStream();
			out=new DataOutputStream(outSocket);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		(new Thread(this)).start();
	}

	public void run() {
		//do stuff with **in** and **out** to interact with client
		while(true){
			try {
				String request=in.readUTF();
				String[] vals=request.split(" ");
				System.out.println(request);

				//Create new table
				if(vals[0].equals("0")){
					TabletServer.table.put(vals[1],new HashMap<String, String>());
				}

				// Insert data into table
				else if(vals[0].equals("1")){
					HashMap<String, String> hmap=TabletServer.table.get(vals[1]);
					hmap.put(vals[2], vals[3]);

					if(hmap.size()>=100){
						FileOutputStream fos = new FileOutputStream("Tablets/"+vals[1]+"_tablet_"+TabletServer.noOfTablets+".txt");

						if(!TabletServer.tabletNumbers.containsKey(vals[1]))
							TabletServer.tabletNumbers.put(vals[1], new ArrayList<Integer>());

						ArrayList<Integer> temp=TabletServer.tabletNumbers.get(vals[1]);
						temp.add(TabletServer.noOfTablets);
						TabletServer.tabletNumbers.put(vals[1],temp);

						List sortedKeys=new ArrayList(hmap.keySet());
						Collections.sort(sortedKeys);

						System.out.println ( sortedKeys.get(0)+" "+sortedKeys.get(99));
						TabletServer.rangeMapping.put("Tablets/"+vals[1]+"_tablet_"+TabletServer.noOfTablets+".txt", 
								new Pair((String) sortedKeys.get(0),(String) sortedKeys.get(99)));

						ObjectOutputStream oos = new ObjectOutputStream(fos);
						oos.writeObject(hmap);
						oos.close();
						fos.close();
						TabletServer.table.put(vals[1],new HashMap<String, String>());

						TabletServer.noOfTablets++;

					}

					else
						TabletServer.table.put(vals[1],hmap);

				}

				// Remove entry from table
				else if(vals[0].equals("2")){
					HashMap<String, String> hmap=TabletServer.table.get(vals[1]);
					
					int f=1;
					if(hmap.containsKey(vals[2])){
						hmap.remove(vals[2]);
						TabletServer.table.put(vals[1],hmap);
						f=0;
						System.out.println("Key removed");
					}
					
					else if(TabletServer.tabletNumbers.containsKey(vals[1])){

						ArrayList<Integer> temp=TabletServer.tabletNumbers.get(vals[1]);

						String key=vals[2];
						for(int i:temp){
							Pair mypair=TabletServer.rangeMapping.get("Tablets/"+vals[1]+"_tablet_"+i+".txt");
							if(key.compareTo(mypair.first)>=0 && key.compareTo(mypair.last)<=0){
								FileInputStream fin=new FileInputStream("Tablets/"+vals[1]+"_tablet_"+i+".txt");
								ObjectInputStream ois=new ObjectInputStream(fin);
								
								try {
									HashMap<String,String> newMap=(HashMap<String,String>) ois.readObject();
									
									if(newMap.containsKey(vals[2])){
										newMap.remove(key);
										FileOutputStream fos = new FileOutputStream("Tablets/"+vals[1]+"_tablet_"+i+".txt");
										
										ObjectOutputStream oos = new ObjectOutputStream(fos);
										oos.writeObject(newMap);
										oos.close();
										fos.close();
										f=0;
										break;
									}
								} catch (ClassNotFoundException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

						}
					}

					if(f==1)
						System.out.println("Key doesn't exist");
				}

				// Get value for a key
				else if(vals[0].equals("3")){
					HashMap<String, String> hmap=TabletServer.table.get(vals[1]);
						
					int f=1;
					
					if(hmap.containsKey(vals[2])){
						String value=hmap.get(vals[2]);
						out.writeUTF(value);
						f=0;
					}

					else if(TabletServer.tabletNumbers.containsKey(vals[1])){

						ArrayList<Integer> temp=TabletServer.tabletNumbers.get(vals[1]);

						String key=vals[2];
						for(int i:temp){
							Pair mypair=TabletServer.rangeMapping.get("Tablets/"+vals[1]+"_tablet_"+i+".txt");
							if(key.compareTo(mypair.first)>=0 && key.compareTo(mypair.last)<=0){
								FileInputStream fin=new FileInputStream("Tablets/"+vals[1]+"_tablet_"+i+".txt");
								ObjectInputStream ois=new ObjectInputStream(fin);
								
								try {
									HashMap<String,String> newMap=(HashMap<String,String>) ois.readObject();
									
									if(newMap.containsKey(vals[2])){
										String value=newMap.get(vals[2]);
										out.writeUTF(value);
										f=0;
										break;
									}
								} catch (ClassNotFoundException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

						}
					}

					if(f==1)
						out.writeUTF("key doesn't exist");
				}

				else{
					out.writeUTF("key doesn't exist");
				}
			}catch (EOFException e) {
			System.out.println("Connection terminated");
			break;
		} 

		catch(IOException e){
			e.printStackTrace();
		}

	}
}
}