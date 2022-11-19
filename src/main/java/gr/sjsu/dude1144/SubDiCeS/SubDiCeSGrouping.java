package gr.sjsu.dude1144.SubDiCeS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class SubDiCeSGrouping implements CustomStreamGrouping, Serializable 
{

	private static final long serialVersionUID = -829509041881995910L;

    private static final Logger LOGGER = Logger.getLogger(SubDiCeSGrouping.class);

	private ArrayList<Integer> all;                  //sorted list of all bolts
    private ArrayList<List<Integer>> shuffled;  //shuffled list of all bolts
    private AtomicInteger current;              //next bolt in the shuffled list
    

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) 
    {
    	
        all = new ArrayList<>(targetTasks);
        Collections.sort(all);
        LOGGER.info("All TASKS: " + all.toString());
    	
        Random random = new Random();
        shuffled = new ArrayList<>(targetTasks.size());
        for (Integer i: targetTasks) {
            shuffled.add(Arrays.asList(i));
        }
        Collections.shuffle(shuffled, random);

        LOGGER.info("SHUFFLED TASKS: " + shuffled.toString());

        current = new AtomicInteger(0);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) 
    {
    	//if this is a community being emitted, emit to the next bolt in the queue
    	if (values.get(1) != null) 
        {
    		int rightNow;
            int size = shuffled.size();
            while (true) 
            {
                rightNow = current.incrementAndGet();
                if (rightNow < size) 
                {
                    return shuffled.get(rightNow);
                } 
                else if (rightNow == size) 
                {
                    current.set(0);
                    return shuffled.get(0);
                }
                //race condition with another thread, and we lost
                // try again
            }
    	}
    	
        //if this is anything other than a community, emit to all bolts
        return all;
    }

}
