package gr.sjsu.dude1144.SubDiCeS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableList;

public class SubDiCeSSpout extends BaseRichSpout
{
    private static final long serialVersionUID = 3100917900801046870L;

	private static final Logger LOGGER = Logger.getLogger(SubDiCeSSpout.class);
	
	private BufferedReader reader;

	private String readLine;
	
	protected static final String PRUNE = "PRUNE";
	
	protected static final String END = "EOF";
	
	protected static final String COMMUNITIES = "COMMUNITIES";

	private SpoutOutputCollector spoutputCollector;

	private boolean active = true;
	
	private boolean initialized = false;

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) 
    {
        spoutputCollector = collector;

        try 
        {
			File graphFile = new File(SubDiCeS.GRAPH_FILE);
			reader = new BufferedReader(new FileReader(graphFile));
		} catch (IOException e) 
        {
			LOGGER.error(SubDiCeS.GRAPH_FILE, e);
		}
    }

    @Override
    public void nextTuple() 
    {
        if (!initialized) 
		{
			ArrayList<SubDiCeSCommunity> SubDiCeSCcommunities = new ArrayList<>();
			try
            {	
				int count = 0;	
				BufferedReader gtcFileBR;
				String readLine;
				gtcFileBR = new BufferedReader(new FileReader(new File(SubDiCeS.GROUND_TRUTH_FILE)));
				
				while ((readLine = gtcFileBR.readLine()) != null) 
                {
					String[] comm = readLine.trim().split(SubDiCeS.GROUND_TRUTH_FILE_DELIMITER);
					Set<Integer> randomNumbers = SubDiCeS.getRandomNumbers(comm.length, SubDiCeS.NUMBER_OF_SEEDS);

					HashSet<String> set = new HashSet<String>();
					for (int number : randomNumbers) 
					{
						set.add(comm[number]);
					}

					SubDiCeSCcommunities.add(new SubDiCeSCommunity(count++, set, comm));	
				}
				gtcFileBR.close();
				LOGGER.info(String.format("%d communities initialized...", SubDiCeSCcommunities.size()));
			} 
			catch (IOException e) 
			{
				LOGGER.error(e.getMessage(), e);
			} 
			catch (Exception e) 
			{
				LOGGER.error(e.getMessage(), e);
			}


			try 
			{
				spoutputCollector.emit(ImmutableList.of(COMMUNITIES, SubDiCeSCcommunities));
			} 
			catch(Exception e) 
			{
				LOGGER.error(e.getMessage(), e);
			} 
			finally 
			{
				initialized = true;
				LOGGER.info(String.format("LogTime: %s", System.currentTimeMillis()));
			}
		}
        
		if (active) 
		{
			try 
			{
				if ((readLine = reader.readLine()) != null) 				//if the next line of the graph isn't null
				{
					String id = UUID.randomUUID().toString();				//create a random ID for the tuple
					spoutputCollector.emit(new Values(readLine, null), id);	//emit the tuple
				} 
				else 
				{
					active = false;
					reader.close();
					spoutputCollector.emit(new Values(END, null));
				}
			} 
			catch (IOException e) 
			{
				LOGGER.error(e);
			}
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declare) 
    {
        declare.declare(new Fields("first", "second"));
    }
    
}