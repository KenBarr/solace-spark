package events;

import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.extern.slf4j.Slf4j;
import payment.Debit;

@Slf4j
public class AppSingleton {
	static private AppSingleton instance = null;
	private EventListener listener = null;
	
	public ConcurrentLinkedQueue<Debit> debits = null;
	
	
	private AppSingleton() {
		this.debits = new ConcurrentLinkedQueue<Debit>();
		log.info("AppSingleton CREATED!!!");
		
	}
	public static AppSingleton getInstance() {
		if (instance == null) {
			
			instance = new AppSingleton();
		}
		return instance;
	}
	
	public void setCallback(EventListener listener){
		this.listener = listener;
		
	}
	public EventListener getCallback(){
		return this.listener;
	}

}
