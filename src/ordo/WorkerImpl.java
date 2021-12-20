package ordo;

import java.io.FileInputStream;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import formats.Format;
import formats.Format.OpenMode;
import hdfs.NameNode;
import map.Mapper;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static int nameNode;
	private static int portWorker;
	private static String nameWorker;
	private static String nameNodeName;
	
	protected WorkerImpl() throws RemoteException {
		super();
	}


	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		Thread lancerMap = new lancementMap(m,reader,writer,cb);
		lancerMap.start();
	}
	
	public class lancementMap extends Thread {
		Mapper m;
		Format reader;
		Format writer;
		CallBack cb;
		
		public lancementMap(Mapper map, Format r, Format w, CallBack callback) {
			this.m = map;
			this.reader = r;
			this.writer = w;
			this.cb = callback;
		}
		
		public void run() {
			// Ouvrir les fichiers 
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);

			// Lancer le map
			m.map(reader, writer);
			//Fin du map : Informer le node initial avec callback
			try {
				cb.FinishMap();
			} catch (RemoteException e) {
				e.printStackTrace();
				reader.close();
				writer.close();
			}			
			
			// Fermer les fichiers
			reader.close();
			writer.close();		
		}
	}
	
    public static void chargerParametresConfig(String path) {
    	try {
    		Properties p = new Properties();
    		p.loadFromXML(new FileInputStream(path));
    		nameNode = Integer.parseInt(p.getProperty("NameNodePort"));
    		portWorker = Integer.parseInt(p.getProperty("WorkerPort"));
    		nameWorker = p.getProperty("WorkerName");
    		nameNodeName = p.getProperty("NameNodeName");
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    
	public static void main (String args[]) {
		try {
    		chargerParametresConfig(args[0]);
			NameNode nNode = (NameNode) Naming.lookup("//"+nameNodeName+":"+nameNode+"/NameNode");
			nNode.registerWorker(nameWorker, portWorker);
			String URL = "//"+nameWorker+":"+portWorker+"/worker";
			LocateRegistry.createRegistry(portWorker);
			Worker w = new WorkerImpl();        
	        Naming.rebind(URL, w);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
