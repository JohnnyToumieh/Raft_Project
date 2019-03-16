import lib.*;

import java.io.*;
import java.rmi.RemoteException;
import java.util.Timer;
import java.util.TimerTask;

public class RaftNode implements MessageHandling {
    private int id;
    private static TransportLib lib;
    private int num_peers;
    
    private int currentTerm;
    private boolean isLeader;
    private Integer votedFor;
    private int currentVotes;

    private static Timer electionTimer;
    private static int electionTimeout;
    private static Timer heartbeatTimer;
    private static int heartbeatTimeout;
    
    private int lastLogIndex;
    private int lastLogTerm;
    private int commitIndex;

    public RaftNode(int port, int id, int num_peers) {
        this.id = id;
        this.num_peers = num_peers;
        lib = new TransportLib(port, id, this);
        
        currentTerm = 0;
        isLeader = false;
        votedFor = null;
        currentVotes = 0;
        
        electionTimer = null;
        electionTimeout = 0;
        heartbeatTimer = null;
        heartbeatTimeout = 0;
        
        lastLogIndex = 0;
        lastLogTerm = 0;
        commitIndex = 0;
    }
    
    public byte[] objToByte(Object object) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
        objStream.writeObject(object);

        return byteStream.toByteArray();
    }
    
    public Object byteToObj(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objStream = new ObjectInputStream(byteStream);

        return objStream.readObject();
    }

    /*
     *call back.
     */
    @Override
    public StartReply start(int command) {
    	System.out.println("StartReply called");
        return null;
    }

    @Override
    public GetStateReply getState() {
    	System.out.println("getStateReply called");
        
    	return new GetStateReply(currentTerm, isLeader);
    }

    @Override
    public Message deliverMessage(Message message) {
		if (message == null) {
			return null;
		}
    	
		Message response = null;
		boolean voteGranted = false;
		boolean success = false;
    	
		try {
	    	if (message.getType() == MessageType.RequestVoteArgs) {
	    		voteGranted = getVote((RequestVoteArgs) byteToObj(message.getBody()));
	    		
	    		byte[] body = objToByte(new RequestVoteReply(currentTerm, voteGranted));
	            response = new Message(MessageType.RequestVoteReply, id, 0, body);
	    	} else if (message.getType() == MessageType.AppendEntriesArgs) {
	    		success = updateLogs((AppendEntriesArgs) byteToObj(message.getBody()));
	    		
	    		byte[] body = objToByte(new AppendEntriesReply(currentTerm, success));
	            response = new Message(MessageType.AppendEntriesReply, id, 0, body);
	    	}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
        return response;
    }
 
    public synchronized boolean updateLogs(AppendEntriesArgs arguments) {
		boolean check = true;
		
		if (check) {
			electionTimer.cancel();
			startElectionTimer(new ElectionTask());
			
			return true;
		}
		
		return false;
    }
    
    public synchronized boolean getVote(RequestVoteArgs arguments) {
		System.out.println("Message from: " + arguments.candidateId 
				+ " | Message to: " + id 
				+ " | Term: " + arguments.term);
		
		boolean voteCheck = votedFor == null || votedFor == arguments.candidateId;
		boolean termCheck = (arguments.term == currentTerm && voteCheck)
							|| arguments.term > currentTerm;
		boolean logCheck = (lastLogTerm < arguments.lastLogTerm 
							|| (lastLogTerm == arguments.lastLogTerm && lastLogIndex <= arguments.lastLogIndex));
		
		if (termCheck && logCheck) {
			System.out.println("Message from: " + arguments.candidateId 
					+ " | Message to: " + id 
					+ " | Term: " + arguments.term
					+ " | Granted");
			votedFor = arguments.candidateId;
			currentTerm = arguments.term;
			
			electionTimer.cancel();
			startElectionTimer(new ElectionTask());
			
			return true;
		} else {
			System.out.println("Message from: " + arguments.candidateId 
					+ " | Message to: " + id 
					+ " | Term: " + arguments.term
					+ " | NOT Granted");
		}
		
		return false;
    }
    
    public synchronized boolean becomeLeader(RequestVoteReply reply) {
    	if (reply.term == currentTerm && reply.voteGranted) {
			currentVotes++;
			
    		System.out.println("User: " + id + " | Votes: " + currentVotes);
			
			if (currentVotes >= num_peers / 2 + 1) {
				isLeader = true;
				
				return true;
			}
    	}
    	
    	return false;
    }
    
    public void sendMessageAll(MessageType type, byte[] body) {
		for (int i = 0; i < num_peers; i++) {
    		if (i != id) {
    			Runnable r = new MessageSender(type, i, body);
    			new Thread(r).start();
    		}
    	}
    }
    
    public class MessageSender implements Runnable {
    	private MessageType type;
    	private int des_add;
    	private byte[] body;
    	
    	public MessageSender(MessageType type, int des_add, byte[] body) {
    		this.type = type;
    		this.des_add = des_add;
    		this.body = body;
		}

    	public void run() {
    		try {
    			Message message = lib.sendMessage(new Message(type, id, des_add, body));
				
    			if (message == null) {
    				return;
    			}
			   
				if (message.getType() == MessageType.RequestVoteReply) {
		    		RequestVoteReply reply = (RequestVoteReply) byteToObj(message.getBody());
		    		
		    		if (becomeLeader(reply)) {
	    				System.out.println("I AM LEADER pepeJAM id: " + id);
	    				
	    				startHeartbeatTimer(new HeartbeatTask(), true);
		    		}
		    	} else if (message.getType() == MessageType.AppendEntriesReply) {
		    		AppendEntriesReply reply = (AppendEntriesReply) byteToObj(message.getBody());
		    		
		    		if (!reply.success) {
	    				
		    		}
		    	}
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
    	}
	}
    
    public static void startElectionTimer(ElectionTask task) {
    	electionTimeout = ((int) Math.random()) * 800 + 300;
    	electionTimer = new Timer();
    	electionTimer.schedule(task, electionTimeout);
    }
    
    public static void startHeartbeatTimer(HeartbeatTask task, boolean doItNow) {
    	heartbeatTimeout = 150;
    	heartbeatTimer = new Timer();
    	if (doItNow) {
    		heartbeatTimer.schedule(task, 0, heartbeatTimeout);
    	} else {
    		heartbeatTimer.schedule(task, heartbeatTimeout);
    	}
    }

    public class HeartbeatTask extends TimerTask {
        @Override
        public void run() {
        	startHeartbeatTimer(new HeartbeatTask(), false);
            
            byte[] body;
            int[] empty = new int[0];
			try {
				body = objToByte(new AppendEntriesArgs(currentTerm, id, lastLogIndex, lastLogTerm, empty, commitIndex));
				sendMessageAll(MessageType.AppendEntriesArgs, body);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }
    
    public class ElectionTask extends TimerTask {
        @Override
        public void run() {
        	currentTerm++;
        	currentVotes = 0;
        	votedFor = id;
        	
        	System.out.println("LEADER MISSING NOTICED REQUESTING FOR VOTES"
					+ " | ID: " + id 
					+ " | Term: " + currentTerm);
        	
        	startElectionTimer(new ElectionTask());
            
            byte[] body;
			try {
				body = objToByte(new RequestVoteArgs(currentTerm, id, lastLogIndex, lastLogTerm));
				sendMessageAll(MessageType.RequestVoteArgs, body);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }

    //main function
    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        //new usernode
        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        
        startElectionTimer(UN.new ElectionTask());
        
        // TODO electionTimer should be reset if a leader sends a heartbeat
    }
}
