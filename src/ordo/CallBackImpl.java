package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;


public class CallBackImpl extends UnicastRemoteObject implements CallBack {

	private static final long serialVersionUID = 1L;

	private Semaphore waitForAllMaps;
		

	protected CallBackImpl() throws RemoteException {
		super();
		waitForAllMaps = new Semaphore(0);
	}

	public void FinishMap() throws RemoteException {
		this.waitForAllMaps.release();
	}
	
	public void waitForRunMap(int nbr) throws RemoteException {
		try {
			this.waitForAllMaps.acquire(nbr);

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
