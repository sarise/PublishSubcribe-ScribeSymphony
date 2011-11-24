package p2p.system.peer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;

public class LongLink
{

	//private ArrayList<PeerAddress> longLinkSet;
	//private ArrayList<Integer> randomIdSet;
	private PeerAddress self;
	private int numberOflongLinks;
	//private int idSpaceSize;
	private int idSpaceSize;
	private int networkSize;
	private PeerAddress succ;
	private PeerAddress pred;

	public LongLink(PeerAddress self, int numberOflongLinks, int idSpaceSize, int networkSize)
	{
		this.self = self;
		this.numberOflongLinks=numberOflongLinks;	// we might not need it.
		this.idSpaceSize=idSpaceSize;
		//longLinkSet=new ArrayList<PeerAddress>();
		//randomIdSet=new ArrayList<Integer>();
		this.networkSize=networkSize;
	}

	public void updatePreSucc(PeerAddress pred, PeerAddress succ)
	{
		this.succ=succ;
		this.pred=pred;
	}

	//Probability Distribution Function
	private double pdf(double n) 
	{
		//System.out.println(" PDF "+Math.exp(Math.log(n)*(Math.random()-1.0)) + "  N: " + n);
		return Math.exp(Math.log(n)*(Math.random()-1.0));
	}

	/*
	private double estimation() 
	{
		if(succ==null || pred==null)
		{
			System.out.println("Class LongLink-estimation() == -1");
			System.exit(1);
			return -1;
		}
		if(pred.equals(self))  return 1.0;
		if(pred.equals(succ)) return 2.0;

		return (2.0*idSpaceSize)/Math.abs(succ.compareByPeerIdTo(pred));	
	}
	*/

	/*
	 * Assumption: It can only handle int primitive variable
	 */
	public int obtainNewLongLinkID() 
	{
		if (pred == null || succ == null)
			return -1;
		
		int I;
		do{

			double ringSize = networkSize;//estimation();

			I = (int) (self.getPeerId().intValue() +  pdf(ringSize)*idSpaceSize) ; //I : Id of the next long link

			if(I>idSpaceSize) I-=idSpaceSize;

			//System.out.println("I="+I + " Pred: "+ pred + " Succ: "+ succ + " RingSize="+ ringSize+" LARGESTID: "+idSpaceSize);
			
		}while(pred.compareByPeerIdTo(I)>0 && succ.compareByPeerIdTo(I)<0);

		return I;
	}

	/*
	private void getAllLongLinksIds()
	{
		randomIdSet.clear();
		//ArrayList<Integer> list=new ArrayList<Integer>();
		for(int i=0;i<this.numberOflongLinks;i++)
		{
			randomIdSet.add(obtainNewLongLinkID());
		}	
		
		Collections.sort(randomIdSet);
		//System.out.println("RandomIdSet: " + randomIdSet);
		
	}
	*/
/*
	public ArrayList<PeerAddress> updateLongLinks(ArrayList<PeerAddress> peers)
	{
		//System.out.println(" LARGESTID: "+idSpaceSize);
		

		int distance;
		int closestDist;
		int candidateDistance;
		int currentDistance;

		if(randomIdSet.size()==0)
		{
			//System.out.println("Class LongLink-updateLongLinks-randomIdSet.size()==0");
			getAllLongLinksIds();
			//return null;
		}
		
		for(int k=0; k < this.numberOflongLinks ; k++)
		{
			PeerAddress closest=peers.get(0);
			closestDist = closest.compareByPeerIdTo(randomIdSet.get(k));

			for(int i=0; i<peers.size(); i++)
			{
				distance = peers.get(i).compareByPeerIdTo(randomIdSet.get(k));
				if( (distance <= 0 && closestDist>0) || (distance <= 0 && closestDist<0 && distance>closestDist ) )
				{
					closest=peers.get(i);
				}	
			}

			if(longLinkSet.size()==k)
			{
				longLinkSet.add(closest);
			}
			else
			{
				currentDistance=longLinkSet.get(k).compareByPeerIdTo(randomIdSet.get(k));
				candidateDistance=closest.compareByPeerIdTo(randomIdSet.get(k));

				if( longLinkSet.get(k)==null || (candidateDistance<0 && (currentDistance<candidateDistance) ) )
					this.longLinkSet.set(k, closest );
			}
		}

		return longLinkSet;
	}
	*/
	

}

