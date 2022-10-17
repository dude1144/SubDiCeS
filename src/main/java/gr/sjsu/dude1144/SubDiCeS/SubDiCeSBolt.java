package gr.sjsu.dude1144.SubDiCeS;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
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

	private static final Values ELEMENT_VALUE = new Values(Integer.valueOf(1));
	
	private static final Values END_VALUE = new Values(SubDiCeSSpout.END);
	
	public static final String DEGREES_SUM = "DEGREES_SUM";
	
	public static final String COMMUNITY_DEGREES_SUM = "COMMUNITY_DEGREES_SUM";

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
				this.communities = (List<SubDiCeSCommunity>) input.getValue(1);
				collector.emit(new Values(communities));
				return;
			}
			
			if (!input.getString(0).equals(SubDiCeSSpout.END)) 
			{
				String[] nodes = input.getString(0).split(SubDiCeS.GRAPH_FILE_DELIMITER);
				// do not allow self loops
				if (nodes[0].equals(nodes[1]))
					return;
			
				//DiCeS.addToDegreeRedis(nodes, async); //increment the degrees for those nodes
				nodeDegrees.merge(nodes[0], 1, Integer::sum);
				nodeDegrees.merge(nodes[1], 1, Integer::sum);
				
				//DiCeS.addToCommRedisPRV(nodes, sync, async, redisCommunities); //add the nodes to the relevent communities
				//collector.emit(ELEMENT_VALUE);
			} 
			else 
			{
				LOGGER.info("Received end message...");
				collector.emit(END_VALUE);
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