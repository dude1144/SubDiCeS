package gr.sjsu.dude1144.SubDiCeS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class SubDiCeS 
{

	private static final Logger LOGGER = Logger.getLogger(SubDiCeS.class);

	public static int BOLTS = 4;

	public static String GRAPH_FILE;

	public static String GROUND_TRUTH_FILE;

	public static String GRAPH_FILE_DELIMITER = " ";

	public static String GROUND_TRUTH_FILE_DELIMITER = " ";

	public enum SizeDetermination 
	{
		GROUND_TRUTH, DROP_TAIL
	}

	public static final int MAX_COMMUNITY_SIZE = 100;
	private static final Random RANDOM = new Random(23);
	public static final int NUMBER_OF_SEEDS = 3;
	public static final int WINDOW_SIZE = 10000; 
	private static final String NODE_COMMUNITIES_PREFIX = "C%s";

	public static SizeDetermination SIZE_DETERMINATION;

	public void execute(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException 
	{
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("SubDiCeS-Spout", new SubDiCeSSpout());
		builder.setBolt("SubDiCes-Bolt", new SubDiCeSBolt()).customGrouping("SubDiCeS-Spout", new SubDiCeSGrouping());

		Config config = new Config();
		config.put(Config.TOPOLOGY_DEBUG, false);
		config.setDebug(false);
		config.setMessageTimeoutSecs(1000);
		config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 4096);
		config.setMaxSpoutPending(15000);

		if (args != null && args.length > 0) 
		{
				
		LOGGER.info("Submitting topology to remote cluster...");

		config.setNumWorkers(BOLTS + 2);
		config.setNumAckers(1);
		StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		} 
		else 
		{
				
			LOGGER.info("Submitting topology to local cluster...");
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("SubDiCeSTopology", config, builder.createTopology());	
						
		}
	}

  	//generate numbersNeeded random numbers, none of which are repeats or greater than max
	//Taken from DiCeS
	public static Set<Integer> getRandomNumbers(int max, int numbersNeeded) 
	{
		if (max < numbersNeeded) 
		{
			throw new IllegalArgumentException("Can't ask for more numbers than are available");
		}
		// Note: use LinkedHashSet to maintain insertion order
		Set<Integer> generated = new LinkedHashSet<>();
		while (generated.size() < numbersNeeded) 
		{
			Integer next = RANDOM.nextInt(max);
			// As we're adding to a set, this will automatically do a containment check
			generated.add(next);
		}
		return generated;
	}

	//taken from DiCeS
	public static byte[] serialize(Object obj) 
	{
        try 
		{
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } 
		catch (IOException e) 
		{
            LOGGER.warn("IO exception while serializing", e);
            return new byte[0];
        }
    }
}
