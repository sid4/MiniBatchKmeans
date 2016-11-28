package edu.iu.km;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CollectiveMapper;
import org.apache.hadoop.mapreduce.Mapper;

import edu.iu.fileformat.Document;
import edu.iu.harp.example.DoubleArrPlus;
import edu.iu.harp.example.IntArrPlus;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;
import edu.iu.harp.resource.IntArray;

public class KmeansMapper extends CollectiveMapper<String, String, Object, Object> {
	private double batchSizeInPercent;
	private int numOfCentroids;
	private int numMapTasks;
	private int iterations;
	private String workingDir;
	private String outputDir;
	private String localDirOutput;
	double classificationAccuracy;

	private static final boolean DEBUG=true;
	private static final String CATEGORY_FILE_NAME="rcv1-v2.topics.qrels";

	protected void setup(Mapper<String, String, Object, Object>.Context context)
			throws IOException, InterruptedException

	{
		Configuration conf = context.getConfiguration();
	    outputDir=conf.get("outputFilePath");
	    localDirOutput=conf.get("localDirOutput");
		iterations = conf.getInt("iterations", 1);
		batchSizeInPercent = conf.getDouble("batchSizeInPercent", 1);
		numOfCentroids = conf.getInt("numOfCentroids", 1);
		workingDir = conf.get("workingDir");
		numMapTasks = conf.getInt("numMapTasks", 1);

	}

	protected void mapCollective(KeyValReader reader,
			Mapper<String, String, Object, Object>.Context context)
			throws IOException, InterruptedException {
		List<String> docFiles = new LinkedList<>();
		while (reader.nextKeyValue()) {
			String key = reader.getCurrentKey();
			// values are filenames
			String value = reader.getCurrentValue();
			docFiles.add(value);
		}
		Configuration conf = context.getConfiguration();
		try {
			runKmeans(docFiles, conf, context);
		} catch (Exception e) {
			System.err.println("Exception occured while running kmeans");
			e.printStackTrace();
		}
	}
	
	private void runKmeans(List<String> fileNames, Configuration conf,
			Mapper<String, String, Object, Object>.Context context)
			throws Exception {
		System.out.println("Starting kmeans");
		final long mapperStart=System.nanoTime();
		Table<Document> centroidTable = new Table<Document>(0, new CentroidCombiner());
		
		//***load documents***
		final long documentLoadStart=System.nanoTime();
		final ConcurrentMap<Integer,Document> documents=Utils.loadDocumentsFromHDFS(fileNames.get(0),conf);
		final long documentLoadEnd=System.nanoTime();
		final long documentLoadTime=TimeUnit.NANOSECONDS.toMillis(documentLoadEnd-documentLoadStart);
		final long documentsCount=documents.size();
		System.out.println(documentsCount+" documents loaded in "+documentLoadTime+ "ms");
		
		//calculate batch size
		int batchSize=(int)((documentsCount*batchSizeInPercent/100)/numMapTasks);
				
		if (isMaster()) {
			populateCentroidTable(centroidTable, numOfCentroids,documents);			
		}
		//broadcast centroids
		final boolean success=broadcast("KMeans","centroidBcast", centroidTable, getMasterID(),false);
		if (DEBUG) {
			//print partitions on each node
			StringBuilder sb = new StringBuilder("Worker ID: " + getSelfID()
					+ "\n");
			for (Partition<Document> parition : centroidTable.getPartitions()) {
				sb.append("partition:" + parition.id() + "\n");
			}
			System.out.println(sb.toString());
		}
		//create a map representative of the centroid table
		ConcurrentMap<Integer,Document> centroidIdToCentroidDocs=centroidTable.getPartitions()
				.stream()
				.parallel()
				.map(Partition::get)
				.collect(Collectors.toConcurrentMap(k->k.getId(), v->v));
		
		List<Document> originalCentroids=centroidIdToCentroidDocs.values()
		.parallelStream().map(centroid->new Document(centroid)).collect(Collectors.toList());
		
		//***calculate initial SSE***
		final long initialSSEStart=System.nanoTime();
		final double initialSSE=calculateSSE(true,documents.values(),centroidIdToCentroidDocs.values());
		final long initialSSECalculationTime=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-initialSSEStart);
		System.out.println("Calculated initial SSE in "+initialSSECalculationTime+ "ms");
		if(isMaster()){
			System.out.println("initial SSE:"+initialSSE);
		}
		
		//***Mini-batch k-means***
		final long miniBatchStart=System.nanoTime();
		final Collection<Document> newCentroids=runMiniBatch(centroidTable,documents,centroidIdToCentroidDocs,batchSize,iterations);
		
		final long miniBatchProcessingTime=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-miniBatchStart);
		System.out.println("Time spent in processing Mini-batch k-means"+miniBatchProcessingTime+ "ms");
		
		if(DEBUG){
			System.out.println("New centroids:");
			System.out.println(newCentroids);
		}
		//***calculate final SSE***
		final long finalSSEStart=System.nanoTime();
		final double finalSSE=calculateSSE(false,documents.values(),newCentroids);
		final long finalSSECalculationTime=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-finalSSEStart);
		System.out.println("Calculated Final SSE in "+finalSSECalculationTime+ "ms");
		System.out.println("final SSE:"+finalSSE);
		
		//***Experimental Classification Accuracy Test***
		System.out.println("Starting classification accuracy test");
		final long classificationAccuracyStart=System.nanoTime();
		final List<Integer> centroidRepresentatorDocumentID=findCentroidRepresentatorDocumentID(newCentroids,documents.values());
		
		final ConcurrentMap<Integer,Integer> docIdTocentroidRepresentatorId=
		associateDocumentWithNearestCentroidRepresentation(documents,centroidRepresentatorDocumentID);

		//IMPORTANT! clearing all the docs, as they are no more required
		documents.clear();
		
		//load categories
		final long categoryFileLoadStart=System.nanoTime();
		ConcurrentMap<Integer,Set<String>> categories= Utils.loadCategoriesFromHDFS(getCategoriesFilePath(workingDir),conf);
		final long categoryFileLoadTime=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-categoryFileLoadStart);
		System.out.println("Time spent in loading categories file:"+categoryFileLoadTime+"ms");
		final long docsCategorizedCorrectly=countDocumentsCategorizedCorrectly(categories,docIdTocentroidRepresentatorId);
		getAggregateAccuracyCounts((int)docsCategorizedCorrectly,docIdTocentroidRepresentatorId.size());
		final long classificationAccuracyCalculationTime=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-classificationAccuracyStart);
		System.out.println("Finished classification accuracy test in:"+classificationAccuracyCalculationTime+"ms");
		context.progress();
		final long timeSpentInMapper=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-mapperStart);
		System.out.println("Total time spent in mapper excluding output write:"+timeSpentInMapper+"ms");
		if(isMaster()){
			writeResults(classificationAccuracy,conf,outputDir,localDirOutput,finalSSE,initialSSE,newCentroids,originalCentroids,iterations,batchSize,batchSizeInPercent,documentsCount,numOfCentroids,numMapTasks,
					documentLoadTime,initialSSECalculationTime,miniBatchProcessingTime,finalSSECalculationTime,classificationAccuracyCalculationTime,timeSpentInMapper,categoryFileLoadTime);
		}
		//centroidTable no more required
				centroidTable.release();
	}
	
	private void writeResults(double classificationAccuracy,Configuration conf,String hdfsOutputDir,String localDirOutput,double finalSSE,double initialSSE,Collection<Document> newCentroids,List<Document> originalCentroids,int iterations,int batchSize,double batchSizeInPercent,long documentsCount,int numOfCentroids,int numMapTasks,
			final long documentLoadTime,final long initialSSECalculationTime,final long miniBatchProcessingTime,final long finalSSECalculationTime,final long classificationAccuracyCalculationTime,final long timeSpentInMapper,final long categoryFileLoadTime) throws IOException{
		StringBuilder output=new StringBuilder();
		String newLine=System.getProperty("line.separator");
		output
		.append("***Configuration***").append(newLine)
		.append("Iterations:").append(iterations).append(newLine)
		.append("Batch Size In Percent:").append(batchSizeInPercent).append(newLine)
		.append("No. of centroids:").append(numOfCentroids).append(newLine)
		.append("No. of map tasks:").append(numMapTasks).append(newLine)
		.append(newLine)
		.append("***Run Statistics***").append(newLine)
		.append("No of documents:").append(documentsCount).append(newLine)
		.append("Batch Size:").append(batchSize*numMapTasks).append(newLine)
		.append("Total time spent in mapper (ms):").append(timeSpentInMapper).append(newLine)
		.append("Time taken to load documents from file (ms):").append(documentLoadTime).append(newLine)
		.append("Initial SSE calculation time (ms):").append(initialSSECalculationTime).append(newLine)
		.append("Time spent in mini-batch(ms):").append(miniBatchProcessingTime).append(newLine)
		.append("Final SSE calculation time (ms):").append(finalSSECalculationTime).append(newLine)
		.append("Time spent in loading category file(ms):").append(categoryFileLoadTime).append(newLine)
		.append("Time spent in calculating classification accuracy:").append(classificationAccuracyCalculationTime).append(newLine)
		.append(newLine)
		.append("***Results***").append(newLine)
		.append("Initial SSE:").append(initialSSE).append(newLine)
		.append("Final SSE:").append(finalSSE).append(newLine)
		.append("Classification Accuracy:").append(classificationAccuracy).append(newLine)
		.append("Initial Centroids:").append(originalCentroids).append(newLine)
		.append("Final Centroids:").append(newCentroids).append(newLine);
		String content=output.toString();
		Utils.writeOutputToHadoopDir(content,hdfsOutputDir,conf);
		Utils.writeOutputToLocalDir(content, localDirOutput);
}
	
	
	
	/**
	 * Count the no. of documents categorized correctly
	 * @param categories Map<DocumentID,<Set of Categories>>
	 * @param docIdTocentroidRepresentatorId 
	 * @return no. of documents categorized correctly
	 */
	private long countDocumentsCategorizedCorrectly(final ConcurrentMap<Integer,Set<String>> categories,final ConcurrentMap<Integer,Integer> docIdTocentroidRepresentatorId){
		System.out.println("Starting to count the documents categorized correctly on the mapper");
		final long categorizedCorrectly=docIdTocentroidRepresentatorId.entrySet()
		.parallelStream()
		.filter(e->categories.get(e.getKey())
				.parallelStream()
				.anyMatch(cat->categories.get(e.getValue()).contains(cat)))
				.count();
		System.out.println("Finished counting the documents categorized correctly on the mapper");
		System.out.println("Docs categorized correctly on the mapper:"+categorizedCorrectly+" out of:"+docIdTocentroidRepresentatorId.size());
		return categorizedCorrectly;

	}
	
	
	private Path getCategoriesFilePath(String workingDir){
		final Path dataDir=new Path (workingDir,"data");
		final Path categoryDir=new Path(dataDir,"categories");
		return new Path(categoryDir,CATEGORY_FILE_NAME);
	}
	
	private void getAggregateAccuracyCounts(int docsCategorizedCorrectly, int docsCount){
		final Table<IntArray> categoryAccuracy = new Table<IntArray>(1, new IntArrPlus());
		categoryAccuracy.addPartition(getCorrectlyCategorizedPartition(docsCategorizedCorrectly));
		categoryAccuracy.addPartition(getTotalDocsPartition(docsCount));
		System.out.println("Aggregating the accuracy results across the mappers at Master");
		reduce("KMeans", "reduce-categorization-accuracy", categoryAccuracy, 0);
		System.out.println("Aggregation complete at Master");
		if(isMaster()){
			double totalDocsCategorizedCorrectly=categoryAccuracy.getPartition(0).get().get()[0];
			double totalDocs=categoryAccuracy.getPartition(1).get().get()[0];
			classificationAccuracy=(totalDocsCategorizedCorrectly/totalDocs);
			System.out.println("Aggregate Category Accuracy:"+classificationAccuracy);
		}
		categoryAccuracy.release();
	}
	/**
	 * 
	 * @param documents documents (from repository)
	 * @param centroidRepresentatorDocumentID  document(from repository) IDs nearest to newly formed centroids
	 * @return Map of <Document ID>,<Nearest Centriod Document Representator ID> 
	 */
	public ConcurrentMap<Integer,Integer> associateDocumentWithNearestCentroidRepresentation(ConcurrentMap<Integer,Document> documents,List<Integer> centroidRepresentatorDocumentID){
		ConcurrentMap<Integer,Integer> docIdTocentroidRepresentatorId=documents
		.values()
		.parallelStream()
		.collect(Collectors.toConcurrentMap(k->k.getId(), v->{
			CentroidComparisonWrapper min=new CentroidComparisonWrapper();
			centroidRepresentatorDocumentID
			.parallelStream()
			.map(id->documents.get(id))
			.forEach(other->{
				min.assignMin(1-v.calculateCosineSimilarity(other), other.getId());
			});
			return min.getClosestCentroidID();
		}));
		return docIdTocentroidRepresentatorId;
	}
	
	/**
	 * Find the documents (from repository) nearest to newly formed centroids. These will act as representative of the centroids
	 * @param newCentroids updated centroid values from Mini-batch k-Means
	 * @param documents documents (from repository)
	 * @return
	 */
	public List<Integer> findCentroidRepresentatorDocumentID(Collection<Document> newCentroids,Collection<Document> documents){
		System.out.println("Finding centroid representators");
		List<Integer> representativeDocumentID= newCentroids
				.parallelStream()
				.map(document -> {
					CentroidComparisonWrapper min = new CentroidComparisonWrapper();
					documents.parallelStream().forEach(
							other -> {
								min.assignMin(1 - document
										.calculateCosineSimilarity(other),
										other.getId());
							});
					return min.getClosestCentroidID();
				}).collect(Collectors.toList());
		System.out.println("Finished finding centroid representators");
		if(DEBUG){
			System.out.println("Documents representing centroids");
			System.out.println(representativeDocumentID);
		}
		return representativeDocumentID;
	}
	/**
	 * Calculate SSE
	 * @param initialSSE is it initial SSE calculation?
	 * @param documents Documents
	 * @param centroidIDToCentroidDoc map of centroid ID to centroid Document
	 * @return
	 */
	public double calculateSSE(boolean initialSSE,Collection<Document> documents,Collection<Document> centroids){
		final ConcurrentMap<Integer, CentroidSimilarityAvg> centroidIDToSimilarityAvg = new ConcurrentHashMap<>();
		//initialize the map
		centroids
		.parallelStream()
		.map(centroid->centroid.getId())
		.forEach(k->centroidIDToSimilarityAvg.put(k, new CentroidSimilarityAvg()));
		//holds the information about the association with closest centroid
		List<DocInfo> docInfo=documents
				.parallelStream()
				.map(doc->{
					CentroidComparisonWrapper min=new CentroidComparisonWrapper();
					centroids
					.parallelStream()
					.forEach(other->{
						min.assignMin(1-doc.calculateCosineSimilarity(other), other.getId());
					});
					centroidIDToSimilarityAvg.get(min.getClosestCentroidID()).add(min.getClosestCentroidSimilarity());
					return new DocInfo(min.getClosestCentroidID(), doc.getId(), min.getClosestCentroidSimilarity());})
				.collect(Collectors.toList());
		ConcurrentMap<Integer,Double> centroidIdToSSEAvg= synchrnonizeAndUpdateCentroidSSEAvg(initialSSE, centroidIDToSimilarityAvg);
		ConcurrentMap<Integer, DoubleAdder> centroidSSE = new ConcurrentHashMap<>();
		//initialize the map
		centroids
		.parallelStream()
		.map(centroid->centroid.getId())
		.forEach(k->centroidSSE.put(k, new DoubleAdder()));
		docInfo
		.parallelStream()
		.forEach(documentInfo->{
			centroidSSE.get(documentInfo.getClosestCentroidID())
			.add(
					Math.pow(documentInfo.getDistanceFromClosestCentroid()
							-centroidIdToSSEAvg.get(documentInfo.getClosestCentroidID()),2));
		});
		double nodeLevelSSE=centroidSSE.values().parallelStream().mapToDouble(adder->adder.doubleValue()).sum();
		double totalSSE=addSSEFromAlltheMappers(nodeLevelSSE);
		return totalSSE;
	}
	
	private double addSSEFromAlltheMappers(double sseVal){
		Table<DoubleArray> summer = new Table<DoubleArray>(3, new DoubleArrPlus());
		DoubleArray sseValPartition=DoubleArray.create(1,false);
		double[] backingArray=sseValPartition.get();
		backingArray[0]=sseVal;
		summer.addPartition(new Partition<DoubleArray>(0,sseValPartition));
		allreduce("KMeans", "reduce-addSSEFromAlltheMappers", summer);
		double totalSSE=summer.getPartition(0).get().get()[0];
		summer.release();
		return totalSSE;
	}
	
	/**
	 * Returns the SSEAvg for each centroid by synchronizing 
	 * the SSEAvg from all the the mappers and calculating the average.
	 * @param intial
	 * @param centroidIDToSimilarityAvg
	 * @param centroidID
	 * @param sum
	 * @param count
	 */
	private ConcurrentMap<Integer,Double> synchrnonizeAndUpdateCentroidSSEAvg(boolean intial,ConcurrentMap<Integer, CentroidSimilarityAvg> centroidIDToSimilarityAvg){
		Table<DoubleArray> categoryAccuracySynchronizer = new Table<DoubleArray>(2, new DoubleArrPlus());
		List<Partition<DoubleArray>> partitions=centroidIDToSimilarityAvg
		.entrySet()
		.parallelStream().map(entry->{
			return getSSEAvgPartition(entry.getKey(),entry.getValue().getSum(),entry.getValue().getCount());
		}).collect(Collectors.toList());
		partitions.stream().forEach(partition->categoryAccuracySynchronizer.addPartition(partition));
		allreduce("KMeans", "reduce-synchrnonizeCentroidSSEAvg"+intial, categoryAccuracySynchronizer);
		ConcurrentMap<Integer,Double> centroidAvgSSE=categoryAccuracySynchronizer
		.getPartitions()
		.parallelStream()
		.collect(Collectors.toConcurrentMap(partition->partition.id(), partition->{
			double updatedSum=partition.get().get()[0];
			double updatedCount=partition.get().get()[1];
			return 	updatedSum/updatedCount;
		}));
		categoryAccuracySynchronizer.release();
		return centroidAvgSSE;
	}
	
	private Partition<DoubleArray> getSSEAvgPartition(int centroidID,double sum, int count){
		DoubleArray sseAvgPartition=DoubleArray.create(2,false);
		double[] backingArray=sseAvgPartition.get();
		backingArray[0]=sum;
		backingArray[1]=count;
		return new Partition<DoubleArray>(centroidID,sseAvgPartition);
	}
	
	private Partition<IntArray> getCorrectlyCategorizedPartition(int docsCategorizedCorrectly){
		IntArray categoryAccuracyArray = IntArray.create(1, false);
		categoryAccuracyArray.get()[0]=docsCategorizedCorrectly;
		return new Partition<IntArray>(0, categoryAccuracyArray);
	}
	private Partition<IntArray> getTotalDocsPartition(int totalDocs){
		IntArray totalDocsArr = IntArray.create(1, false);
		totalDocsArr.get()[0]=totalDocs;
		return new Partition<IntArray>(1, totalDocsArr);
	}
	


private  Collection<Document> runMiniBatch(Table<Document> centroidTable,ConcurrentMap<Integer,Document> documents, ConcurrentMap<Integer,Document> centroids, int batchSize,int iterations){
	System.out.println("Mini-batch k-means started");
	//ConcurrentMap<Integer,Document> centroids=new ConcurrentHashMap<>(originalCentroids);
	IntStream
	.range(0, iterations)
	.peek(i -> {
					if (DEBUG) {
						System.out.println("mini-batch iteration " + i);
					}
				})
	.forEach(i->{
		//get a batch of documents for the iteration
		if (DEBUG) {
			System.out.println("generating batch");
		}
		List<Document> batch=Utils.getBatch(documents,batchSize);
		if (DEBUG) {
			System.out.println("batch generated");
		}
		//<docID,centroidID>
		if (DEBUG) {
			System.out.println("populating centroid cache");
		}
		ConcurrentMap<Integer,Integer> centroidCache=buildCentroidCache(batch,centroids.values());
		if (DEBUG) {
			System.out.println("Completed creating centroid cache");
		}
		
		//keep only the centroids which are to be processed by this mapper
		if (DEBUG) {
			System.out.println("Removing centroids which are not be processed on this mapper");
		}
		List<Integer> removePartition=new LinkedList<>();
		IntIterator partitionIDIterator=centroidTable.getPartitionIDs().iterator();
		while(partitionIDIterator.hasNext()){
			Integer parttionID=partitionIDIterator.next();
			if(! ((parttionID % getNumWorkers())==getSelfID())){
				removePartition.add(parttionID);
			}	
		}
		removePartition.stream().forEach(parttionID->{
			centroids.remove(centroidTable.getPartition(parttionID).get().getId());
			centroidTable.removePartition(parttionID);
		});
		//only the centroids which are to be processed by this mapper are left
		if (DEBUG) {
			System.out.println("Removed centroids which are not be processed on this mapper");
		}
		//update centroids according to the algo and rotate them so that each mapper gets the opportunity
		IntStream
		.range(0,getNumWorkers())
		.peek(it->{if(DEBUG)System.out.println("Updating centroids -> "+ it);})
		.forEach(j->{
			batch
			//this can not be done in multi-threaded way, do not make it parallel!!
			.stream()
			.forEach(document->{
				Document centroid=centroids.get(centroidCache.get(document.getId()));
				if(centroid==null){
					//the centroid is not to be processed by this node
					return;
				}
				else{
					int count=centroid.getPointsAssociatedCount().incrementAndGet();
					double n=(double)1/count;
					BiFunction<Integer,Double,Double> reMappingFunction=(featureID,featureVal)->featureVal*(1-n)+n*document.getFeatureValue(featureID);
					centroid.updateFeatureValues(reMappingFunction);
				}
			});
			if (DEBUG) {
				System.out.println("Rotating centroid model");
				System.out.println("Partitions before rotation");
				centroidTable.getPartitions()
				.stream().forEach(partition->System.out.println(partition.get().getId()));
			}
			
			rotate("KMeans", "roatateCentroids-"+"i-"+j, centroidTable, null);
			if (DEBUG) {
				System.out.println("Centroid model rotation complete");
				System.out.println("Partitions after rotation");
				centroidTable.getPartitions()
				.stream().forEach(partition->System.out.println(partition.get().getId()));
			}
			//clear old centroid data in map
			centroids.clear();
			//populate new centroid from table
			centroidTable.getPartitions()
			.stream()
			.parallel()
			.map(Partition::get)
			.forEach(centroid->centroids.put(centroid.getId(), centroid));
		});
		if (DEBUG) {
			System.out.println("Invoking all gather");
		}
		//gather all the centroids on each mapper
			allgather("KMMapper","allGather-"+i,centroidTable);
			//update the centroids map
			centroidTable.getPartitions()
			.stream()
			.parallel()
			.map(Partition::get)
								.forEach(
										centroid -> {
											if (!centroids.containsKey(centroid.getId())) {
												centroids.put(centroid.getId(),centroid);
											}
										});

			if (DEBUG) {
				System.out.println("Iteration complete");
			}	
	});
	System.out.println("Mini-batch k-means ended");
	return centroids.values();
}  
private ConcurrentMap<Integer,Integer> buildCentroidCache(List<Document> batch,Collection<Document> centroids){
	return
	batch
	.parallelStream()
	.distinct()
	.collect(Collectors.toConcurrentMap(
			document->document.getId(),
			document->{
		CentroidComparisonWrapper min=new CentroidComparisonWrapper();
		centroids
		.parallelStream()
		//can be further parallelised using reduction, look into it
		.forEach(d->{
			min.assignMin(1-d.calculateCosineSimilarity(document),d.getId());});
			return min.getClosestCentroidID();
}));
}
/**
 * Randomly choose documents as centroid from the documents repo and populates the centroid table with them
 * @param cenTable
 * @param numOfCentroids
 * @param documents
 */
	private void populateCentroidTable(Table<Document> cenTable, int numOfCentroids,ConcurrentMap<Integer,Document> documents) {
		System.out.println("constructing centroid table created");
		ConcurrentMap<Integer,Document> centroids=Utils.getRandomCentroids(numOfCentroids, documents);
		int[] i=new int[1];
		centroids
		.values()
		.stream()
		.forEach(doc->{cenTable.addPartition(new Partition<Document>(i[0]++, doc));});
		System.out.println("centroid table created");
	}
	
	public static class CentroidSimilarityAvg{
		private DoubleAdder sum;
		private LongAdder count;
		public CentroidSimilarityAvg(){
			sum=new DoubleAdder();
			count=new LongAdder();
		}
		/**
		 * adds a value and increments the counter
		 * @param val similarity value
		 */
		public void add(double val){
			sum.add(val);
			count.increment();
		}
		
		public double getSum() {
			return sum.doubleValue();
		}
		
		public int getCount() {
			return count.intValue();
		}
		
		
		
	}
	
	public static class DocInfo {
		private int closestCentroidID;
		private int docID;
		private double distanceFromClosestCentroid;
		public DocInfo(int closestCentroidID, int docID,
				double distanceFromClosestCentroid) {
			super();
			this.closestCentroidID = closestCentroidID;
			this.docID = docID;
			this.distanceFromClosestCentroid = distanceFromClosestCentroid;
		}
		public int getClosestCentroidID() {
			return closestCentroidID;
		}
		public void setClosestCentroidID(int closestCentroidID) {
			this.closestCentroidID = closestCentroidID;
		}
		public int getDocID() {
			return docID;
		}
		public void setDocID(int docID) {
			this.docID = docID;
		}
		public double getDistanceFromClosestCentroid() {
			return distanceFromClosestCentroid;
		}
		public void setDistanceFromClosestCentroid(double distanceFromClosestCentroid) {
			this.distanceFromClosestCentroid = distanceFromClosestCentroid;
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
		//should not be used in multithreaded env when the values are getting updated
		public double getClosestCentroidSimilarity() {
			return distance;
		}	
		
	}
}
