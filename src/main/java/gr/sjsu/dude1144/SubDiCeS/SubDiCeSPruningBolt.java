package gr.sjsu.dude1144.SubDiCeS;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class SubDiCeSPruningBolt extends BaseBasicBolt 
{

	private static final long serialVersionUID = -4482496627316555773L;

	private static final Logger LOGGER = Logger.getLogger(SubDiCeSPruningBolt.class);
	
	private int endCounter = 0;
	
	private int communityCounter = 0;
	
	public double f1scoreTotal = 0D;

	public static double f1score = 0D;

	public static Boolean END_INDICATOR = false;

	public static int numEdgesProcessed = 0;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) 
	{
		
		Object message = input.getValue(0);
		
		if (message instanceof Double) 
		{
			communityCounter++;
			//LOGGER.info("COMMUNITY COUNTER " + communityCounter);
			f1scoreTotal += (Double)message;
		} 
		else if(message instanceof Integer)
		{
			if(SubDiCeSPruningBolt.numEdgesProcessed == 0)
				SubDiCeSPruningBolt.numEdgesProcessed = (Integer)message;
		}
		else if (message instanceof String) 
		{
			endCounter++;
			if (endCounter == SubDiCeS.BOLTS) 
			{
				LOGGER.info("ALL BOLTS FINISHED ");
				f1score = f1scoreTotal / communityCounter;
				LOGGER.info(String.format("F1 score: %f", f1score));
				LOGGER.info(String.format("LogTime: %s", System.currentTimeMillis()));

				END_INDICATOR = true;
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {}

}
