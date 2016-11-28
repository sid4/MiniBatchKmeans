package edu.iu.km;

import edu.iu.fileformat.Document;
import edu.iu.harp.partition.PartitionCombiner;
import edu.iu.harp.partition.PartitionStatus;

public class CentroidCombiner extends PartitionCombiner<Document>{
	public PartitionStatus combine(Document curPar, Document newPar) {
		return PartitionStatus.COMBINED;
	}
}
/*
public class DoubleArrPlusextends PartitionCombiner<DoubleArray>{
public PartitionStatuscombine(DoubleArraycurPar, DoubleArraynewPar) {
double[] doubles1 = curPar.get(); intsize1 = curPar.size();
double[] doubles2 = newPar.get(); intsize2 = newPar.size();
if (size1 != size2) {returntatus.COMBINE_FAILED;
} PartitionS
for (inti= 0; i< size2; i++) {
doubles1[i] = doubles1[i] + doubles2[i];
}
return PartitionStatus.COMBINED;
}}
*/