package ut.distcomp.framework;

import java.awt.TrayIcon.MessageType;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import dc.Action;
import dc.Message;
import dc.Message.NodeType;
import dc.Action.ActionType;

public class MainTest {
	public static void main(String[] args)
	{
		Config controllerConfig = null;
		Config process = null;
		try {
			controllerConfig = new Config("config_p0.txt");
			process = new Config("config_p1.txt");
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();
		LinkedBlockingQueue<Message>[] queues = new LinkedBlockingQueue[5];
		for(int i=0; i<queues.length; i++){
		    queues[i]=new LinkedBlockingQueue<Message>(); //change constructor as needed
		}

		NetController controller = new NetController(controllerConfig, queues);
		NetController p1 = new NetController(process, queue, queue, queue, queue, queue);
		controller.sendMsg(1, new Message(0,1,NodeType.CONTROLLER, NodeType.COORDINATOR, new Action(ActionType.VOTE_REQ, null),System.currentTimeMillis()));
		p1.sendMsg(0, new Message(0,1,NodeType.PARTICIPANT, NodeType.CONTROLLER, new Action(ActionType.VOTE_REQ, null),System.currentTimeMillis()));
		//System.out.println("P1 messga:"+queue.size());
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		controller.getReceivedMsgs();
		System.out.println("P1 messga:"+queue.size());
		System.out.println("No of message in Queue "+queues[1].element().toString() );
		
	}
}
