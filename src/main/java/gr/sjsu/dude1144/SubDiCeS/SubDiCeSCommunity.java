package gr.sjsu.dude1144.SubDiCeS;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import clojure.lang.Sorted;

public class SubDiCeSCommunity implements Serializable
{
	private class SortedPair implements Comparable<SortedPair>
	{
		public String value;
		public double score;

		public SortedPair(String value, double score)
		{
			this.value = value;
			this.score = score;
		}

		
		public int compareTo(SortedPair other)
		{
			return this.score > other.score ? 1 : (this.score < other.score ? -1 : 0); 
		}

		@Override
		public boolean equals(Object o)
		{
			if(!(o instanceof SortedPair))
				return false;
		
			return this.value.equals(((SortedPair)o).value);// && this.score == ((SortedPair)o).score;
		}
	}

    private static long serialVersionUID = 8941749640675016290L;

    private Set<String> seedSet;
	private String[] groundTruth;
	private String id;
	private int intId;

	
	private HashMap<String, Integer> commDegrees;
	private SortedSet<SortedPair> nodesInCommunity;

    public SubDiCeSCommunity(int id, Set<String> seedSet)
	{
		this(id, seedSet, null);
	}

    public SubDiCeSCommunity(int id, Set<String> seedSet, String[] groundTruth)
    {
		this.intId = id;
		this.id = String.format("RC%d", id);
		if(seedSet == null)
			throw new IllegalArgumentException();
		this.seedSet = new HashSet<>();
		for(String seed : seedSet)
		{
			this.seedSet.add(seed);
			this.nodesInCommunity.add(new SortedPair(seed, 1D));
		}
		this.groundTruth = groundTruth;
		this.commDegrees = new HashMap<>();
	}
}
