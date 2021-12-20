package hdfs;

import java.io.FileInputStream;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import formats.Format.Type;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode {

	private static final long serialVersionUID = 1L;
	public static Map<String,Integer> dataNodes = new HashMap<>();
	public static int NameNodePort;
	public static Map<String,ArrayList<String>> files = new HashMap<>();
	public static Map<String,Type> fileType = new HashMap<>();
	public static Map<String,Integer> workers = new HashMap<>();
	public static String machine;
	
	protected NameNodeImpl() throws RemoteException {
		
	}
	
	@Override
	public void registerDaemon(String ip, int port) throws RemoteException {
		dataNodes.put(ip, port);
	}
	@Override
	public String[] getAllDaemons() throws RemoteException {
		String deamon;
		String[] tableau = new String[dataNodes.size()];
		int pos = 0;
		for (String i:dataNodes.keySet())  {
			deamon = "://"+i+"/"+dataNodes.get(i);
			tableau[pos] = deamon;
			pos += 1;
		}
		return tableau;
	}
	@Override
	public boolean removeDaemon(String ip) throws RemoteException {
		if (dataNodes.containsKey(ip)) {
			dataNodes.remove(ip);
			return true;
		} else {
		return false;
		}
	}
	
    private static void paramsManquants() {
		System.out.println("Commande à faire : java NameNode <fichier de configuration>");	
	}
	
    public static void chargerParametresConfig(String path) {
    	try {
    		Properties p = new Properties();
    		p.loadFromXML(new FileInputStream(path));
    		NameNodePort = Integer.parseInt(p.getProperty("NameNodePort"));
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
            LocateRegistry.createRegistry(NameNodePort);
            NameNode nNode = new NameNodeImpl();
            Naming.rebind("//"+machine+":" + NameNodePort + "/NameNode", nNode);
            System.out.println("NameNode: Ecoute sur le port " + NameNodePort);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	@Override
	public Map<String, Integer> getDataNodes() throws RemoteException {
		return dataNodes;
	}

	@Override
	public Type getType(String file) throws RemoteException {
		Type t = fileType.get(file);
		if (t==null) {
			return Type.KV;
		} else {
			return t;
		}
	}

	@Override
	public void ajoutFichier(String file, Type t) throws RemoteException {
		fileType.put(file, t);
		ArrayList<String> serveurs = new ArrayList<String>();
        Iterator<String> keysDataNodes = dataNodes.keySet().iterator();
		while(keysDataNodes.hasNext()) {
			String serv = keysDataNodes.next();
			serveurs.add(serv);
		}
		files.put(file, serveurs);
	}

	@Override
	public Map<String, Integer> getWorkers() throws RemoteException {
		return workers;
	}

	@Override
	public void registerWorker(String name, int port) throws RemoteException {
		workers.put(name, port);
	}

}
