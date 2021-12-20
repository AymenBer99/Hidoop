package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.util.Properties;

import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
public class HdfsServer {
	
	public static int nameNode;
	public static String serverName;
	public static int port;
	public static String machine;

    private static void paramsManquants() {
		System.out.println("Commande à faire : java HdfsServer <fichier de configuration>");	
	}
	
    public static void chargerParametresConfig(String path) {
    	try {
    		Properties p = new Properties();
    		p.loadFromXML(new FileInputStream(path));
    		nameNode = Integer.parseInt(p.getProperty("NameNodePort"));
    		port = Integer.parseInt(p.getProperty("Port"));
    		serverName = p.getProperty("ServerName");
    		machine = p.getProperty("NameNodeName");
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
	
    
    public static void main(String[] args) {
		if (args.length != 1) {
			paramsManquants();
			return;
		}
		chargerParametresConfig(args[0]);
		try {
			NameNode nNode = (NameNode) Naming.lookup("//"+machine+":"+nameNode+"/NameNode");
			nNode.registerDaemon(serverName, port);
			ServerSocket sSocket = new ServerSocket(port);
            System.out.println("HdfsServer: Ecoute sur le port " + port);
            while(true) {
            	Socket hdfsClient = sSocket.accept();
            	TraitementDemande demande = new TraitementDemande(hdfsClient);
            	demande.start();
            }
		} catch(Exception e) {
			e.printStackTrace();
		}
    }
    
    public static class TraitementDemande extends Thread {
    	
    	Socket client;
    	
    	public TraitementDemande(Socket c) {
    		client = c;
    	}
    	
    	public void run() {
    		try {
    			InputStream is = client.getInputStream();
    			ObjectInputStream ois = new ObjectInputStream(is);
    			OutputStream os = client.getOutputStream();
    			ObjectOutputStream oos = new ObjectOutputStream(os);
    			String cmd = (String) ois.readObject();
    			Type type = null;
    			String nomFichier = null;
    			switch (cmd) {
    			case "DELETE":
    				String file = (String) ois.readObject();
    				(new File(file)).delete();
    				break;
    			case "WRITE":
    				type = (Type) ois.readObject();
    				nomFichier = (String) ois.readObject();	
    				Format fileToWrite = null;
    				switch (type) {
    				case LINE:
    					fileToWrite = new LineFormat(nomFichier);
    					break;
    				case KV:
    					fileToWrite = new KVFormat(nomFichier);
    					break;
    				default:
    					throw new RuntimeException("Erreur type de fichier");
    				}
    				fileToWrite.open(OpenMode.W);
    				KV receivedMessage;
    				do {
    					receivedMessage = (KV) ois.readObject();
    					if (receivedMessage != null) {
    						fileToWrite.write(receivedMessage);
    					}
    				} while(receivedMessage != null);
    				fileToWrite.close();
    				break;
    			case "READ":
        			type = (Type) ois.readObject();
    				nomFichier = (String) ois.readObject();	
    				Format fileToRead = null;
    				switch (type) {
    				case LINE:
    					fileToRead = new LineFormat(nomFichier);
    					break;
    				case KV:
    					fileToRead = new KVFormat(nomFichier);
    					break;
    				default:
    					throw new RuntimeException("Erreur type de fichier");
    				}
    				fileToRead.open(OpenMode.R);
    				KV messageToSend;
    				while ((messageToSend = fileToRead.read()) != null) {
    					oos.writeObject(new KV(messageToSend.k,messageToSend.v));
    				}
    				oos.writeObject(null);
    				fileToRead.close();
    				break;
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}
    		
    }
}


