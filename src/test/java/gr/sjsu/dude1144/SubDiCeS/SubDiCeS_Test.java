package gr.sjsu.dude1144.SubDiCeS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

import org.apache.log4j.Logger;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.Map;

public class SubDiCeS_Test 
{
  private static Logger LOGGER = Logger.getLogger(SubDiCeS_Test.class);

    
  // @Test
  // public void communityTest() 
  // {
  //   HashSet<String> seedSet = new HashSet<String>(); 
  //   seedSet.add("node0");
  //   seedSet.add("node1");
  //   seedSet.add("node2");

  //   SubDiCeSCommunity testCommunity = new SubDiCeSCommunity(0, seedSet);

  //   assertTrue(testCommunity.getCommunitySize() == 3);

  //   System.out.println(testCommunity.getCommunityDegreesSize());
  //   System.out.println(testCommunity.toString());

  //   for(int i = 3; i < 105; i++)
  //   {
  //     testCommunity.processNodes("node" + (i-1), "node" + i, 2, 1);
  //   }

  //   assertTrue(testCommunity.getCommunitySize() == 105);

  //   System.out.println(testCommunity);

  //   testCommunity.prune();

  //   System.out.println(testCommunity);

  //   assertTrue(testCommunity.getCommunitySize() == 100);
  // }

  @Test
  public void amazonTest()  throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException 
  {
    double expectedF1Score = 0.80;
    String filename = "amazon";

    SubDiCeS subdices = new SubDiCeS();
    SubDiCeS.GRAPH_FILE = Resources.getResource(filename + ".txt").getFile();
    SubDiCeS.GROUND_TRUTH_FILE = Resources.getResource(filename + "GTC.txt").getFile();
    SubDiCeS.SIZE_DETERMINATION = SubDiCeS.SizeDetermination.DROP_TAIL;
    SubDiCeSCommunity.logCommunity = false;
    SubDiCeSCommunity.logPrunedCommunity = false;

    long startTime = System.nanoTime();

    subdices.execute(null);

    while(!SubDiCeSPruningBolt.END_INDICATOR)
    {
			TimeUnit.MILLISECONDS.sleep(100);
		}
    
    long endTime = System.nanoTime();
		long estimatedTime = (endTime - startTime)/1000L;

    assertEquals(expectedF1Score, SubDiCeSPruningBolt.f1score, 0.05);
    LOGGER.info("Estimated Time: " + startTime);
    LOGGER.info("Estimated Time: " + endTime);
    LOGGER.info("Estimated Time: " + estimatedTime);
    LOGGER.info("Time per edge:  " + estimatedTime / (SubDiCeSPruningBolt.numEdgesProcessed)  + " microseconds (" + SubDiCeSPruningBolt.numEdgesProcessed + ") edges");   
  }

  @Test
  public void youtubeTest()  throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException 
  {
    double expectedF1Score = 0.10;
    String filename = "youtube";

    SubDiCeS subdices = new SubDiCeS();
    SubDiCeS.GRAPH_FILE = Resources.getResource(filename + ".txt").getFile();
    SubDiCeS.GROUND_TRUTH_FILE = Resources.getResource(filename + "GTC.txt").getFile();
    SubDiCeS.SIZE_DETERMINATION = SubDiCeS.SizeDetermination.DROP_TAIL;
    SubDiCeSCommunity.logCommunity = false;
    SubDiCeSCommunity.logPrunedCommunity = false;

    long startTime = System.nanoTime();

    subdices.execute(null);

    while(!SubDiCeSPruningBolt.END_INDICATOR)
    {
			TimeUnit.MILLISECONDS.sleep(100);
		}

    long endTime = System.nanoTime();
		long estimatedTime = (endTime - startTime)/1000L;

    assertEquals(expectedF1Score, SubDiCeSPruningBolt.f1score, 0.05);

    LOGGER.info("Estimated Time: " + startTime);
    LOGGER.info("Estimated Time: " + endTime);
    LOGGER.info("Estimated Time: " + estimatedTime);
    LOGGER.info("Time per edge:  " + estimatedTime / (SubDiCeSPruningBolt.numEdgesProcessed)  + " microseconds (" + SubDiCeSPruningBolt.numEdgesProcessed + ") edges");    
  }

  @Test
  public void dblpTest()  throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException 
  {
    double expectedF1Score = 0.40;
    String filename = "dblp";

    SubDiCeS subdices = new SubDiCeS();
    SubDiCeS.GRAPH_FILE = Resources.getResource(filename + ".txt").getFile();
    SubDiCeS.GROUND_TRUTH_FILE = Resources.getResource(filename + "GTC.txt").getFile();
    SubDiCeS.SIZE_DETERMINATION = SubDiCeS.SizeDetermination.DROP_TAIL;
    SubDiCeSCommunity.logCommunity = false;
    SubDiCeSCommunity.logPrunedCommunity = false;

    long startTime = System.nanoTime();

    subdices.execute(null);

    while(!SubDiCeSPruningBolt.END_INDICATOR)
    {
			TimeUnit.MILLISECONDS.sleep(100);
		}

    long endTime = System.nanoTime();
		long estimatedTime = (endTime - startTime)/1000L;

    assertEquals(expectedF1Score, SubDiCeSPruningBolt.f1score, 0.05);

    LOGGER.info("Estimated Time: " + startTime);
    LOGGER.info("Estimated Time: " + endTime);
    LOGGER.info("Estimated Time: " + estimatedTime);
    LOGGER.info("Time per edge:  " + estimatedTime / (SubDiCeSPruningBolt.numEdgesProcessed)  + " microseconds (" + SubDiCeSPruningBolt.numEdgesProcessed + ") edges");   
  }

  @Test
  public void livejournalTest()  throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException 
  {
    double expectedF1Score = 0.60;
    String filename = "livejournal";

    SubDiCeS subdices = new SubDiCeS();
    SubDiCeS.GRAPH_FILE = Resources.getResource(filename + ".txt").getFile();
    SubDiCeS.GROUND_TRUTH_FILE = Resources.getResource(filename + "GTC.txt").getFile();
    SubDiCeS.SIZE_DETERMINATION = SubDiCeS.SizeDetermination.DROP_TAIL;
    SubDiCeSCommunity.logCommunity = false;
    SubDiCeSCommunity.logPrunedCommunity = false;

    long startTime = System.nanoTime();

    subdices.execute(null);

    while(!SubDiCeSPruningBolt.END_INDICATOR)
    {
			TimeUnit.MILLISECONDS.sleep(100);
		}

    long endTime = System.nanoTime();
		long estimatedTime = (endTime - startTime)/1000L;

    assertEquals(expectedF1Score, SubDiCeSPruningBolt.f1score, 0.05);

    LOGGER.info("Estimated Time: " + startTime);
    LOGGER.info("Estimated Time: " + endTime);
    LOGGER.info("Estimated Time: " + estimatedTime);
    LOGGER.info("Time per edge:  " + estimatedTime / (SubDiCeSPruningBolt.numEdgesProcessed)  + " microseconds (" + SubDiCeSPruningBolt.numEdgesProcessed + ") edges");   
  }

  @Test
  public void orkutTest()  throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException 
  {
    double expectedF1Score = 0.41;
    String filename = "orkut";

    SubDiCeS subdices = new SubDiCeS();
    SubDiCeS.GRAPH_FILE = Resources.getResource(filename + ".txt").getFile();
    SubDiCeS.GROUND_TRUTH_FILE = Resources.getResource(filename + "GTC.txt").getFile();
    SubDiCeS.SIZE_DETERMINATION = SubDiCeS.SizeDetermination.DROP_TAIL;
    SubDiCeSCommunity.logCommunity = false;
    SubDiCeSCommunity.logPrunedCommunity = false;

    long startTime = System.nanoTime();

    subdices.execute(null);

    while(!SubDiCeSPruningBolt.END_INDICATOR)
    {
			TimeUnit.MILLISECONDS.sleep(100);
		}

    long endTime = System.nanoTime();
		long estimatedTime = (endTime - startTime)/1000L;

    assertEquals(expectedF1Score, SubDiCeSPruningBolt.f1score, 0.05);

    LOGGER.info("Estimated Time: " + startTime);
    LOGGER.info("Estimated Time: " + endTime);
    LOGGER.info("Estimated Time: " + estimatedTime);
    LOGGER.info("Time per edge:  " + estimatedTime / (SubDiCeSPruningBolt.numEdgesProcessed)  + " microseconds (" + SubDiCeSPruningBolt.numEdgesProcessed + ") edges");   
  }

  @After
	public void cleanUp() throws InterruptedException 
  {
    // re-initialize the f1score variable that acts as a flag for termination
    SubDiCeSPruningBolt.f1score = 0;
    SubDiCeSPruningBolt.numEdgesProcessed = 0;
    SubDiCeSPruningBolt.END_INDICATOR = false;
	}
}
