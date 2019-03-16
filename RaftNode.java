import lib.*;

import java.io.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class RaftNode implements MessageHandling {
    private int id;
    private static TransportLib lib;
    private int num_peers;
    
    private int currentTerm;
    private Integer votedFor;
    private ArrayList<LogEntry> log;

    private volatile int commitIndex;
    private volatile int lastApplied;
    
    private volatile ArrayList<Integer> nextIndex;
    private volatile ArrayList<Integer> matchIndex;

    private volatile boolean isLeader;
    private volatile int currentVotes;

    private volatile Timer electionTimer;
    private volatile ElectionTask electionTask;
    private volatile int electionTimeout;
    private volatile Timer heartbeatTimer;
    private volatile HeartbeatTask heartbeatTask;
    private volatile int heartbeatTimeout;

    public RaftNode(int port, int id, int num_peers) {
        this.id = id;
        this.num_peers = num_peers;
        lib = new TransportLib(port, id, this);
        
        currentTerm = 0;
        votedFor = null;
        log = new ArrayList<LogEntry>();
        
        commitIndex = 0;
        lastApplied = 0;
        
        nextIndex = new ArrayList<Integer>();
        for (int i = 0; i < num_peers; i++) {
        	nextIndex.add(1);
        }
        
        matchIndex = new ArrayList<Integer>();
        for (int i = 0; i < num_peers; i++) {
        	nextIndex.add(0);
        }

        isLeader = false;
        currentVotes = 0;
        
        electionTimer = null;
        electionTimeout = 0;
        heartbeatTimer = null;
        heartbeatTimeout = 0;
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
    	System.out.println("getStateReply called" 
				+ " | User: " + id 
				+ " | Term: " + currentTerm
				+ " | isLeader: " + isLeader);
        
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
    	
		// The node receives either a request for votes message or append entry message
		// It also needs to return the reply to the sender
		
		try {
	    	if (message.getType() == MessageType.RequestVoteArgs) {
	    		voteGranted = doSyncWork(byteToObj(message.getBody()), MessageType.RequestVoteArgs);
	    		
	    		byte[] body = objToByte(new RequestVoteReply(currentTerm, voteGranted));
	            response = new Message(MessageType.RequestVoteReply, id, 0, body);
	    	} else if (message.getType() == MessageType.AppendEntriesArgs) {
	    		success = doSyncWork(byteToObj(message.getBody()), MessageType.AppendEntriesArgs);
	    		
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
    
    public synchronized boolean doSyncWork(Object obj, MessageType type) {
    	if (type == MessageType.AppendEntriesArgs) {
    		return updateLogs((AppendEntriesArgs) obj);
    	} else if (type == MessageType.RequestVoteArgs) {
    		return getVote((RequestVoteArgs) obj);
    	} else if (type == MessageType.RequestVoteReply) {
    		return becomeLeader((RequestVoteReply) obj);
    	}
    	
    	return false;
    }
 
    // This does synchronized work when an Append Entry Request is sent
    public synchronized boolean updateLogs(AppendEntriesArgs arguments) {
		boolean check = arguments.term >= currentTerm;
		
		//System.out.println("HBR: " + arguments.leaderId + " to " + id);
		
		if (check) {
			if (isLeader) {
				isLeader = false;
				
				heartbeatTimer.cancel();
				heartbeatTask.cancel();
				
				System.out.println("CANCELLING BEING LEADER: " + arguments.leaderId + " to " + id);
			} else {
				electionTimer.cancel();
				electionTask.cancel();
			}
	    	electionTimer = new Timer();
			startElectionTimer();
			
			return true;
		}
		
		return false;
    }
    
    // This does synchronized work when an Request Vote Request is sent
    public synchronized boolean getVote(RequestVoteArgs arguments) {
		System.out.println("Message from: " + arguments.candidateId 
				+ " | Message to: " + id 
				+ " | Term: " + arguments.term);
		
		boolean voteCheck = votedFor == null || votedFor == arguments.candidateId;
		boolean termCheck = (arguments.term == currentTerm && voteCheck)
							|| arguments.term > currentTerm;
		
		
		int lastLogIndex = 0;
		int lastLogTerm = 0;
		if (log.size() != 0) {
			lastLogIndex = log.size() - 1;
			lastLogTerm = (log.get(lastLogIndex)).term;
		}
		
		boolean logCheck = (lastLogTerm < arguments.lastLogTerm 
							|| (lastLogTerm == arguments.lastLogTerm && lastLogIndex <= arguments.lastLogIndex));
		
		if (termCheck && logCheck) {
			System.out.println("Message from: " + arguments.candidateId 
					+ " | Message to: " + id 
					+ " | Term: " + arguments.term
					+ " | Granted");
			votedFor = arguments.candidateId;
			currentTerm = arguments.term;
			
			if (isLeader) {
				isLeader = false;
				heartbeatTimer.cancel();
				heartbeatTask.cancel();
			} else {
				electionTimer.cancel();
				electionTask.cancel();
			}
	    	electionTimer = new Timer();
			startElectionTimer();
			
			return true;
		} else {
			System.out.println("Message from: " + arguments.candidateId 
					+ " | Message to: " + id 
					+ " | Term: " + arguments.term
					+ " | NOT Granted");
		}
		
		return false;
    }
    
    // This does synchronized work when the response to the Request Vote is received
    public synchronized boolean becomeLeader(RequestVoteReply reply) {
    	if (reply.term == currentTerm && reply.voteGranted) {
			currentVotes++;
			
    		System.out.println("User: " + id + " | Votes: " + currentVotes);
			
			if (currentVotes == num_peers / 2 + 1) {
				isLeader = true;
				electionTimer.cancel();
				electionTask.cancel();
		    	heartbeatTimer = new Timer();
				startHeartbeatTimer(true);
				
				return true;
			}
    	}
    	
    	return false;
    }
    
    // This function tries to broadcast a message for all servers
    // It creates a new thread for each communication because we need to wait for the reply
    public void sendMessageAll(MessageType type, byte[] body) {
		for (int i = 0; i < num_peers; i++) {
    		if (i != id) {
    			Runnable r = new MessageSender(type, i, body);
    			new Thread(r).start();
    		}
    	}
    }
    
    // This is the thread that handles sending messages for a server
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
			   
    			// The node receives a reply for his request
    			
				if (message.getType() == MessageType.RequestVoteReply) {
		    		RequestVoteReply reply = (RequestVoteReply) byteToObj(message.getBody());
		    		
		    		if (doSyncWork(reply, MessageType.RequestVoteReply)) {
	    				System.out.println("I AM LEADER pepeJAM id: " + id);
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
    
    // This starts the election timer
    public void startElectionTimer() {
    	if (isLeader) {
    		return;
    	}
    	
    	electionTimeout = (int) (Math.random() * 700 + 300);
    	electionTask = new ElectionTask();
    	electionTimer.schedule(electionTask, electionTimeout);
    }
    
    // This starts the heartbeat timer with the option to send a heartbeat right away
    public void startHeartbeatTimer(boolean doItNow) {
    	heartbeatTimeout = 150;
    	heartbeatTask = new HeartbeatTask();
    	if (doItNow) {
    		heartbeatTimer.schedule(heartbeatTask, 0, heartbeatTimeout);
    	} else {
    		heartbeatTimer.schedule(heartbeatTask, heartbeatTimeout);
    	}
    }

    // This is the heartbeat
    public class HeartbeatTask extends TimerTask {
        @Override
        public void run() {
        	if (!isLeader) {
            	System.out.println("WHO IS CALLING ME " + id);
        		return;
        	}
        	
        	startHeartbeatTimer(false);
            
        	ArrayList<LogEntry> empty = new ArrayList<LogEntry>();
			try {
				int lastLogIndex = 0;
				int lastLogTerm = 0;
				if (log.size() != 0) {
					lastLogIndex = log.size() - 1;
					lastLogTerm = (log.get(lastLogIndex)).term;
				}
				
				byte[] body = objToByte(new AppendEntriesArgs(currentTerm, id, lastLogIndex, lastLogTerm, empty, commitIndex));
				sendMessageAll(MessageType.AppendEntriesArgs, body);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }
    
    // This is the election
    public class ElectionTask extends TimerTask {
        @Override
        public void run() {
        	currentTerm++;
        	currentVotes = 1;
        	votedFor = id;
        	
        	System.out.println("LEADER MISSING NOTICED REQUESTING FOR VOTES"
					+ " | ID: " + id 
					+ " | Term: " + currentTerm);
        	
        	startElectionTimer();
            
			try {
				int lastLogIndex = 0;
				int lastLogTerm = 0;
				if (log.size() != 0) {
					lastLogIndex = log.size() - 1;
					lastLogTerm = (log.get(lastLogIndex)).term;
				}
				
				byte[] body = objToByte(new RequestVoteArgs(currentTerm, id, lastLogIndex, lastLogTerm));
				sendMessageAll(MessageType.RequestVoteArgs, body);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        
        Thread thread = new Thread() {
        	public void run() {
            	UN.electionTimer = new Timer();
                UN.startElectionTimer();
        	}
        };

        thread.start();
    }
}
