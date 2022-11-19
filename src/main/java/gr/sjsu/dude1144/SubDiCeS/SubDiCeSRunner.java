package gr.sjsu.dude1144.SubDiCeS;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

public class SubDiCeSRunner 
{

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException 
    {
		new SubDiCeS().execute(args);
	}

}