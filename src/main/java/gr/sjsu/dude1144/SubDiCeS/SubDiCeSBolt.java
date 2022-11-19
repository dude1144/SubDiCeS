package gr.sjsu.dude1144.SubDiCeS;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SubDiCeSBolt extends BaseBasicBolt
{

    private static final Logger LOGGER = Logger.getLogger(SubDiCeSBolt.class);
	
	private static final long serialVersionUID = 2582648360999730523L;
	
	public static final String DEGREES_SUM = "DEGREES_SUM";
	
	public static final String COMMUNITY_DEGREES_SUM = "COMMUNITY_DEGREES_SUM";

	public static Boolean logNodeDegrees = false;

	private static Integer logging = 0;

	private int boltID;

	private int edgesSeen = 0;

    private List<SubDiCeSCommunity> communities;

	private HashMap<String, Integer> nodeDegrees;

	private HashMap<String, ArrayList<SubDiCeSCommunity>> invertedIndex;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) 
    {
        try 
		{
			if (input.getValue(1) != null) 
			{
				this.communities.add((SubDiCeSCommunity)input.getValue(1));
				return;
			}
			
			if (!input.getString(0).equals(SubDiCeSSpout.END)) 
			{
				if(edgesSeen == 0)
				{
					LOGGER.info("Bolt@" + boltID + ": " + this.communities.size() + " communities initialized");

					String comms = "";

					for(SubDiCeSCommunity comm : this.communities)
					{
						comms += (comms.equals("")) ? "[" : ", ";
						comms += comm.getID();
					}

					comms += "]";

					LOGGER.info(comms);
				}
				edgesSeen++;

				String[] nodes = input.getString(0).split(SubDiCeS.GRAPH_FILE_DELIMITER);
				// do not allow self loops
				if (nodes[0].equals(nodes[1]))
					return;
			
				//DiCeS.addToDegreeRedis(nodes, async); //increment the degrees for those nodes
				nodeDegrees.merge(nodes[0], 1, Integer::sum);
				nodeDegrees.merge(nodes[1], 1, Integer::sum);
				
				//DiCeS.addToCommRedisPRV(nodes, sync, async, redisCommunities); //add the nodes to the relevent communities
				if(edgesSeen % SubDiCeS.WINDOW_SIZE == 0)
					LOGGER.info("Bolt@" + boltID + ": Pruning after " + edgesSeen + "edges");
				for(SubDiCeSCommunity comm : this.communities)
				{
					if(comm.contains(nodes[0]) || comm.contains(nodes[1]))
						comm.processNodes(nodes[0], nodes[1], nodeDegrees.get(nodes[0]), nodeDegrees.get(nodes[1]));
					if(edgesSeen % SubDiCeS.WINDOW_SIZE == 0)
						comm.prune();

				}
			} 
			else 
			{
				LOGGER.info("Received end message...");

				if(logNodeDegrees && logging++ == 0)
				{
					List<String> s = this.nodeDegrees.entrySet().stream().map(e->{return "[" + e.getKey() + "," + e.getValue() + "]";}).collect(Collectors.toList());
					LOGGER.info(s);
				}

				LOGGER.info("EDGES SEEN: " + edgesSeen);
				collector.emit(new Values(new Integer(edgesSeen)));

				for(SubDiCeSCommunity comm : communities)
				{
					switch(SubDiCeS.SIZE_DETERMINATION)
					{
						case GROUND_TRUTH:
							collector.emit(new Values(comm.getGroundTruthSizeDetermination()));
							break;
						case DROP_TAIL:
						default:
							collector.emit(new Values(comm.getDropTailSizeDetermination()));
							break;
						
					}
				}
				collector.emit(new Values(SubDiCeSSpout.END));
			}
		} 
		catch (Exception e) 
		{
			LOGGER.error("EXC: ", e);
		}
        
    }

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
		declarer.declare(new Fields("command"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) 
	{
		this.communities = new ArrayList<>();
		this.nodeDegrees = new HashMap<>();
		this.boltID = context.getThisTaskId();
		LOGGER.info("Initialized empty communities' list");
		
		// String communities = sync.get(SubDiCeSSpout.COMMUNITIES);
		// if (communities != null && !communities.isEmpty()) {
		// 	redisCommunities = (List<RedisCommunity>) DiCeS.deserialize(Base64.getDecoder().decode(communities));
		// 	LOGGER.info(String.format("Retrieved %d communities", redisCommunities.size()));
		// } 
		// else 
		// {
		// 	this.communities = new ArrayList<>();
		// 	LOGGER.info("Initialized empty communities' list");
		// }
		
		//LOGGER.info("Bolt initialized...");
	}
}