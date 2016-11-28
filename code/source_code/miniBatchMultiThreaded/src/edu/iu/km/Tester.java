package edu.iu.km;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.iu.fileformat.Document;

public class Tester {
	private static final boolean UNIX_ENV=true;
	private static final boolean KARST_ENV=false;

	private static String DATA_PATH;
	private static String CATEGORY_PATH;
	static{
		if(UNIX_ENV){
			DATA_PATH="/u/jain8/assignments/ds/term/data/vector/lyrl2004_vectors_test_pt0.dat";
			CATEGORY_PATH="/u/jain8/assignments/ds/term/data/rcv1-v2.topics.qrels";
	}
		else if(KARST_ENV){
			DATA_PATH="/N/u/jain8/Karst/term/data/lyrl2004_vectors_test_pt0.dat";
			CATEGORY_PATH="/N/u/jain8/Karst/term/data/rcv1-v2.topics.qrels";
		
		}
		else{
			DATA_PATH="C:/Users/Siddharth Jain/Documents/Assignments/Distributed Systems/term project/vector_data/lyrl2004_vectors_test_pt0.dat";
			CATEGORY_PATH="C:/Users/Siddharth Jain/Documents/Assignments/Distributed Systems/term project/vector_data/topics/rcv1-v2.topics.qrels";
		}
		
	}
	
	public static class  CentroidComparisonWrapper{
		private volatile double distance;
		private volatile int documentID;
		public CentroidComparisonWrapper(){
			distance=Double.MAX_VALUE;
			documentID=Integer.MIN_VALUE;
		}
		public synchronized void assignMin(double otherDistance,int otherID){
			if(otherDistance<distance){
				distance=otherDistance;
				documentID=otherID;
			}
		}
		//should not be used in multithreaded env
		public int getClosestCentroidID(){
			return documentID;
		} 
		
	}
public static void main(String... args) throws IOException{
	long start=System.currentTimeMillis();
	
	ConcurrentMap<Integer,Document> documents= Utils.loadDocuments(DATA_PATH);
	System.out.println(documents.size());
	ConcurrentMap<Integer,Set<String>> categories= Utils.loadCategories(CATEGORY_PATH);
	//System.out.println(categories);
	ConcurrentMap<Integer,Document> cetroids=Utils.getRandomCentroids(7,documents);
	
//	Document d=new Document(1,IntStream
//							.range(1,5)
//							.boxed()
//							.collect(Collectors.toMap(k->k,k->Double.valueOf(k))));
	//System.out.println(d.calculateCosineSimilarity(d));

	//System.out.println(documents.get(0).calculateCosineSimilarity(documents.get(0)));
	
	Collection<Document> newCetroids=runMiniBatch(documents,cetroids,40000,400);
	
	List<Integer> centroidRepresentator=newCetroids.stream().map(document->{
		CentroidComparisonWrapper min=new CentroidComparisonWrapper();
		documents.values().parallelStream().forEach(other->{
			min.assignMin(1-document.calculateCosineSimilarity(other), other.getId());
		});
		return min.getClosestCentroidID();
	}).collect(Collectors.toList());
	System.out.println("Documents representing centroids");
	System.out.println(centroidRepresentator);
	ConcurrentMap<Integer,Integer> docIdTocentroidRepresentatorId=documents.values()
	.parallelStream()
	.collect(Collectors.toConcurrentMap(k->k.getId(), v->{
		CentroidComparisonWrapper min=new CentroidComparisonWrapper();
		centroidRepresentator
		.parallelStream()
		.map(id->documents.get(id))
		.forEach(other->{
			min.assignMin(1-v.calculateCosineSimilarity(other), other.getId());
		});
		return min.getClosestCentroidID();
	}));
	
	double accuracy=((double)(docIdTocentroidRepresentatorId.entrySet()
	.parallelStream()
	.filter(e->categories.get(e.getKey())
			.parallelStream()
			.anyMatch(cat->categories.get(e.getValue()).contains(cat)))
			.count()))/docIdTocentroidRepresentatorId.size();
	System.out.println("accuracy:"+accuracy);
//	System.out.println("DOC ID -> Representative Document ID");
//	System.out.println(docIdTocentroidRepresentatorId);
	System.out.println(System.currentTimeMillis()-start);
	System.out.println("complete");
	
}

private static Collection<Document> runMiniBatch(Map<Integer,Document> documents, ConcurrentMap<Integer,Document> centroids, int batchSize,int iterations){
	IntStream
	.range(0, iterations)
	.forEach(i->{
		//get a batch of documents for the iteration
		List<Document> batch=Utils.getBatch(documents,batchSize);
		//<docID,centroidID>
		ConcurrentMap<Integer,Integer> centroidCache=
		batch
		.parallelStream()
		.distinct()
		.collect(Collectors.toConcurrentMap(
				document->document.getId(),
				document->{
			CentroidComparisonWrapper min=new CentroidComparisonWrapper();
			centroids
			.values()
			.stream()
			//can be further parallelised using reduction, look into it
			.forEach(d->{
				min.assignMin(1-d.calculateCosineSimilarity(document),d.getId());});
				return min.getClosestCentroidID();
}));
		
		Map<Integer,AtomicInteger> centroidCount=centroids.
				keySet().
				stream().
				collect(Collectors.toMap(k->k,v->new AtomicInteger()));
		//System.out.println(centroidCount);
		batch
		.stream()
		.forEach(document->{
//			System.out.println(centroids);
//			System.out.println(document.getId());
//			System.out.println(centroidCache);
//			System.out.println(centroidCache.get(document.getId()));
			Document centroid=centroids.get(centroidCache.get(document.getId()));
			//System.out.println(centroid.getId());
			int count=centroidCount.get(centroid.getId()).incrementAndGet();
			double n=(double)1/count;
			BiFunction<Integer,Double,Double> reMappingFunction=(featureID,featureVal)->featureVal*(1-n)+n*document.getFeatureValue(featureID);
			centroid.updateFeatureValues(reMappingFunction);
		});
	});
	return centroids.values();
}  

//in util


private static ConcurrentMap<Integer,Set<String>> loadCategories() throws IOException{
	final Function<Matcher,String> baseCategoryExtractor = m -> m.group("baseCategory");
	final Function<Matcher,Integer> docIDExtractor = m -> Integer.parseInt(m.group("docid"));
	
	Pattern pattern=Pattern.compile("^(?<category>(?<baseCategory>\\w{1})(\\w+))\\s(?<docid>\\d+)\\s1$");
	ConcurrentMap<Integer,Set<String>> categories=Files.lines(Paths.get(CATEGORY_PATH))
		.parallel()
		//split each line by spaces
		.map(s->{
			Matcher matcher=pattern.matcher(s);
			matcher.find();
			return matcher;})
		.collect(Collectors.groupingByConcurrent(docIDExtractor,Collectors.mapping(baseCategoryExtractor,Collectors.toSet())));
		return categories;
}


	private static Document getRandomDocument(Map<Integer, Document> documents) {
		Document random = null;
		try {
			Field table = ConcurrentHashMap.class.getDeclaredField("table");
			table.setAccessible(true);
			Entry<Integer, Document>[] entries = (Entry<Integer, Document>[]) table
					.get(documents);
			while (random == null) {
				int index = (int) Math.floor(documents.size() * Math.random());
				while(index>=entries.length){
					index = (int) Math.floor(documents.size() * Math.random());
				}
				if (entries[index] != null) {
					random = (Document) entries[index].getValue();
				}
			}
		} catch (NoSuchFieldException | IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
		return new Document(random);
	}



}
