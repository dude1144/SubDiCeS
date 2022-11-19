package gr.sjsu.dude1144.SubDiCeS;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import clojure.lang.Sorted;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubDiCeSCommunity implements Serializable
{
    private static long serialVersionUID = 8941749640675016290L;

	private static final Logger LOGGER = Logger.getLogger(SubDiCeSCommunity.class);

	public static Boolean logCommunity = false;
	public static Boolean logPrunedCommunity = false;

    private Set<String> seedSet;
	private String[] groundTruth;
	private String id;
	private int intId;

	
	private HashMap<String, Double> commDegrees;
	//private TreeMap<SortedPair, SortedPair> nodesInCommunity;
	private HashMap<String, Double> nodesInCommunity;

    public SubDiCeSCommunity(int id, Set<String> seedSet)
	{
		this(id, seedSet, null);
	}

    public SubDiCeSCommunity(int id, Set<String> seedSet, String[] groundTruth)
    {
		this.intId = id;
		this.id = String.format("RC%d", id);
		//this.nodesInCommunity = new TreeMap<>(new SortedPairComparator());
		this.nodesInCommunity = new HashMap<>();
		if(seedSet == null)
			throw new IllegalArgumentException();
		this.seedSet = new HashSet<>();
		for(String seed : seedSet)
		{
			this.seedSet.add(seed);
			//SortedPair toAdd = new SortedPair(seed, 1D);
			//this.nodesInCommunity.put(toAdd, toAdd);
			this.nodesInCommunity.put(seed, 1D);
		}
		if(groundTruth != null)
			this.groundTruth = groundTruth;
		this.commDegrees = new HashMap<>();
	}

	public String getID()
	{
		return this.id;
	}

	public Boolean contains(String node)
	{
		return this.nodesInCommunity.containsKey(node);
	}

	public void processNodes(String nodeU, String nodeV, int degreesNodeU, int degreesNodeV)
	{
		Double scoreNodeU = null;
		Double scoreNodeV = null;
		boolean commContainsNodeU = false;
		boolean commContainsNodeV = false;
			
		double commDegreeSum = 0D;
		double degreeSum = 0D;

		if(this.nodesInCommunity.containsKey(nodeU))
		{
			scoreNodeU = this.nodesInCommunity.get(nodeU);
			commContainsNodeU = scoreNodeU != null;
		}

		if(this.nodesInCommunity.containsKey(nodeV))
		{
			scoreNodeV = this.nodesInCommunity.get(nodeV);
			commContainsNodeV = scoreNodeV != null;
		}

		if(this.seedSet.contains(nodeU))
		{
			commDegrees.merge(nodeV, 1D, Double::sum);
		}
		else if (commContainsNodeU)
		{
			commDegrees.merge(nodeV, scoreNodeU, Double::sum);
		}

		if(this.seedSet.contains(nodeV))
		{
			commDegrees.merge(nodeU, 1D, Double::sum);
		}
		else if (commContainsNodeV)
		{
			commDegrees.merge(nodeU, scoreNodeV, Double::sum);
		}

		if(commContainsNodeU && !seedSet.contains(nodeV))
		{
			double degreeV = degreesNodeV + degreeSum;
			Double temp = commDegrees.get(nodeV);
			double commDegreeV = temp != null ? temp + commDegreeSum : 0D + commDegreeSum;
			this.nodesInCommunity.put(nodeV, commDegreeV/ degreeV);
			commContainsNodeV = true;
		}
		if(commContainsNodeV && !seedSet.contains(nodeU))
		{
			double degreeU = degreesNodeU + degreeSum;
			Double temp = commDegrees.get(nodeU);
			double commDegreeU = temp != null ? temp + commDegreeSum : 0D + commDegreeSum;
			this.nodesInCommunity.put(nodeU, commDegreeU/ degreeU);
			commContainsNodeU = true;
		}
	}

	public void prune()
	{
		this.prune(SubDiCeS.MAX_COMMUNITY_SIZE);
	}

	public void prune(int pruneToValue)
	{
		int nodesToRemove = nodesInCommunity.size() - SubDiCeS.MAX_COMMUNITY_SIZE;
		if(nodesToRemove > 0)
		{
			if(logPrunedCommunity)
			{
				LOGGER.info("\tComm@" + this.id + ": removing " + nodesToRemove + " nodes");
				List<String> sortedList = this.nodesInCommunity.entrySet().stream().sorted(new Comparator<Map.Entry<String, Double>>() 
				{
					public int compare(Map.Entry<String, Double> o1,  Map.Entry<String, Double> o2)
					{
						return (o2.getValue()).compareTo(o1.getValue());
					}
				}).map(e -> {return "[" + e.getKey() + "," + e.getValue() + "]";}).collect(Collectors.toList());

				List<String> prunedList = this.nodesInCommunity.entrySet().stream().sorted(new Comparator<Map.Entry<String, Double>>() 
				{
					public int compare(Map.Entry<String, Double> o1,  Map.Entry<String, Double> o2)
					{
						return (o2.getValue()).compareTo(o1.getValue());
					}
				}).limit(pruneToValue).map(e -> {return "[" + e.getKey() + "," + e.getValue() + "]";}).collect(Collectors.toList());

				LOGGER.info("BEFORE PRUNING: " + sortedList.toString());
				LOGGER.info("AFTER  PRUNING: " + prunedList.toString());

			}

			this.nodesInCommunity = this.nodesInCommunity.entrySet().stream().sorted(new Comparator<Map.Entry<String, Double>>() 
			{
				public int compare(Map.Entry<String, Double> o1,  Map.Entry<String, Double> o2)
				{
					return (o2.getValue()).compareTo(o1.getValue());
				}
			}).limit(pruneToValue).collect(Collectors.toMap(e -> e.getKey(), e->  e.getValue(), (v1, v2) -> {throw new IllegalStateException();}, () -> new HashMap<>()));
			
			for(String seed : this.seedSet)
			{
				nodesInCommunity.put(seed, 1D);
			}

			return;
		}
	}

	public int getCommunitySize()
	{
		return this.nodesInCommunity.size();
	}

	public int getCommunityDegreesSize()
	{
		return this.commDegrees.size();
	}


	public static double getF1Score(Set<String> foundCommunities, String[] gtc)
	{
		HashSet<String> groundTruthCommunities = new HashSet<String>(Arrays.asList(gtc));
		SetView<String> commonCommunities = Sets.intersection(foundCommunities, groundTruthCommunities);
		
		double precision = (double) commonCommunities.size() / foundCommunities.size();
		double recall = (double) commonCommunities.size() / groundTruthCommunities.size();
		
		if (precision == 0 && recall == 0)
			return 0;
		else
			return 2 * (precision * recall) / (precision + recall);
	}

	public double getF1Score()
	{
		HashSet<String> groundTruthCommunities = new HashSet<String>(Arrays.asList(this.groundTruth));
		Set<String> foundCommunities = this.nodesInCommunity.keySet();
		SetView<String> commonCommunities = Sets.intersection(foundCommunities, groundTruthCommunities);
		
		double precision = (double) commonCommunities.size() / foundCommunities.size();
		double recall = (double) commonCommunities.size() / groundTruthCommunities.size();
		
		if (precision == 0 && recall == 0)
			return 0;
		else
			return 2 * (precision * recall) / (precision + recall);
	}

	public  double getGroundTruthSizeDetermination() 
	{
		int bestSize = Math.min(this.groundTruth.length, this.nodesInCommunity.size());
		double f1score = 0D;
		try 
		{
			f1score = getF1Score(this.getPrunedCommunity(bestSize), this.groundTruth);
		} catch (Exception e) 
		{
			LOGGER.error(e);
		}
		return f1score;
	}

	public double getDropTailSizeDetermination() 
	{
		int bestSize = 0;

		List<Entry<String, Double>> sortedCommunity = this.nodesInCommunity.entrySet().stream().sorted(new Comparator<Map.Entry<String, Double>>() 
		{
			public int compare(Map.Entry<String, Double> o1,  Map.Entry<String, Double> o2)
			{
				return (o2.getValue()).compareTo(o1.getValue());
			}
		}).collect(Collectors.toList());

		if(logCommunity)
			LOGGER.info(sortedCommunity);
		
		// calculate mean distance
		Double previous = null;
		double totalDifference = 0D;
		int totalDifferenceCount = 0;

		for (int i = 2 * SubDiCeS.NUMBER_OF_SEEDS; i < sortedCommunity.size(); i++) 
		{
			Entry<String, Double> entry = sortedCommunity.get(i);
			if (previous != null && !this.seedSet.contains(entry.getKey())) 
			{
				totalDifference += previous - entry.getValue();
				totalDifferenceCount++;
			}
			previous = entry.getValue();
		}
		
		double meanDifference = 0D;
		if (totalDifferenceCount != 0) 
		{
			meanDifference = totalDifference / totalDifferenceCount;			
		}

		List<Entry<String, Double>> reverseSortedCommunity = this.nodesInCommunity.entrySet().stream().sorted(new Comparator<Map.Entry<String, Double>>() 
		{
			public int compare(Map.Entry<String, Double> o1,  Map.Entry<String, Double> o2)
			{
				return (o2.getValue()).compareTo(o1.getValue());
			}
		}).collect(Collectors.toList());

		Collections.reverse(reverseSortedCommunity);
		previous = null;
		int tailSize = 0;
		for (Entry<String, Double> entry : reverseSortedCommunity) 
		{
			tailSize++;
			if (previous != null) {
				double difference = entry.getValue() - previous;
				if (difference > meanDifference) 
				{
					break;
				}
			}
			previous = entry.getValue();
		}

		bestSize = this.nodesInCommunity.size() - tailSize;
		LOGGER.info(this.intId + ": GT: " + this.groundTruth.length + " DT: " + bestSize + " CS: " + (bestSize+tailSize));

		double f1score = 0D;
		try 
		{
			f1score = getF1Score(this.getPrunedCommunity(bestSize), this.groundTruth);
		} catch (Exception e) 
		{
			LOGGER.error(e);
		}
		return f1score;
	}

	public List<Map.Entry<String, Double>> getSortedCommunity()
	{
		return this.nodesInCommunity.entrySet().stream().sorted(new Comparator<Map.Entry<String, Double>>() 
		{
			public int compare(Map.Entry<String, Double> o1,  Map.Entry<String, Double> o2)
			{
				return (o2.getValue()).compareTo(o1.getValue());
			}
		}).collect(Collectors.toList());
	}

	public Set<String> getPrunedCommunity(int pruneTo)
	{
		return this.nodesInCommunity.entrySet().stream().sorted(new Comparator<Map.Entry<String, Double>>() 
		{
			public int compare(Map.Entry<String, Double> o1,  Map.Entry<String, Double> o2)
			{
				return (o2.getValue()).compareTo(o1.getValue());
			}
		}).map(e->{return e.getKey();}).limit(pruneTo).collect(Collectors.toSet());
	}

	public String toString()
	{
		String toReturn = "Comm@" + this.id + " - " + this.nodesInCommunity.size();

		Set<Map.Entry<String, Double>> entries = this.nodesInCommunity.entrySet();

		for (Map.Entry<String, Double> entry : entries) 
		{
            toReturn += "\n\t" + entry.getKey() + ": " + entry.getValue();
        }

		return toReturn;
	}


}
