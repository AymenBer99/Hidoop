package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote {

	public void FinishMap() throws RemoteException;

	public void waitForRunMap(int nbr) throws RemoteException;
}
