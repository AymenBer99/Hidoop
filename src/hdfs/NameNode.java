package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

import formats.Format.Type;

public interface NameNode extends Remote {
		
    void registerDaemon(String ip,int port) throws RemoteException;

    String[] getAllDaemons() throws RemoteException;

    boolean removeDaemon(String ip) throws RemoteException;
    
    Map<String,Integer> getDataNodes() throws RemoteException;
    
    Type getType(String file) throws RemoteException;

    void ajoutFichier(String file, Type t) throws RemoteException;
    
    Map<String,Integer> getWorkers() throws RemoteException;

    void registerWorker(String ip,int port) throws RemoteException;

}