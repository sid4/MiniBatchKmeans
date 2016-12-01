package edu.iu.km;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.iu.fileformat.Document;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;

public class Utils { 
	private static final int POINT_GENERATION_RANGE=100;
	static void generateData(int numOfDataPoints, int vectorSize, int numMapTasks, 
			FileSystem fs,  String localDirStr, Path pointsDir) throws IOException, InterruptedException, ExecutionException {
		//create local files
		List<java.nio.file.Path> localFilePaths=new LinkedList<>();
		//String time=Long.toString(System.currentTimeMillis());
		List<StringBuilder> localFileData=new ArrayList<>();
		for(int i=0;i<numMapTasks;i++){
			java.nio.file.Path path=Paths.get(localDirStr,"kmeans-data-"+i);
			localFilePaths.add(path);
			localFileData.add(new StringBuilder());
		}
		
		//generate points
		for (int i=0;i<numOfDataPoints;i++){
			StringBuilder fileData=localFileData.get(i%numMapTasks);
			for(int dimension=0;dimension<vectorSize-1;dimension++){
				fileData.append(Math.random()*POINT_GENERATION_RANGE);
				fileData.append(' ');
			}
			fileData.append(Math.random()*POINT_GENERATION_RANGE);
			if(i+numMapTasks<numOfDataPoints){
				fileData.append(System.lineSeparator());
			}
		}
		//delete existing dir
		if(fs.exists(pointsDir)){
			fs.delete(pointsDir,true);
		}
		//create dir in hdfs
		fs.mkdirs(pointsDir);
		//write to local fir and copy to hdfs
		for(int i=0;i<numMapTasks;i++){
			//write to local dir
			Files.write(localFilePaths.get(i), localFileData.get(i).toString().getBytes());
			//copy to hdfs
			fs.copyFromLocalFile(new Path(localFilePaths.get(i).toRealPath().toString()), pointsDir);
		}
	
	}
	public static void copyCategories(FileSystem fs, String localDirStr,
			Path categoriesDir){
		copyFromLocalFile(fs,localDirStr,categoriesDir);
	}
	public static void copyDocuments(FileSystem fs, String localDirStr,
			Path docDir)  {
		copyFromLocalFile(fs,localDirStr,docDir);
	}
	
	private static void copyFromLocalFile(FileSystem fs, String localDirStr,
			Path hdfsDestinationDir)  {
		try (Stream<java.nio.file.Path> files = Files.walk(Paths.get(localDirStr)).filter(Files::isRegularFile)) {
			if (fs.exists(hdfsDestinationDir)) {
				fs.delete(hdfsDestinationDir, true);
			}
			// create dir in hdfs
			fs.mkdirs(hdfsDestinationDir);
			files.forEach(path -> {
				try {
					fs.copyFromLocalFile(new Path(path.toRealPath().toString()), hdfsDestinationDir);
				} catch (IOException e) {
					System.err.print("Failed copying file:"+path.toString());
					e.printStackTrace();
				}});
		} catch (IllegalArgumentException|IOException e) {
			System.err.print("Exception occured while copying files:");
			e.printStackTrace();
		}
	}
	
	
	public static ConcurrentMap<Integer,Document> loadDocuments(String path) throws IOException{
		ConcurrentMap<Integer,Document> documents=Files.lines(Paths.get(path))
			.parallel()
			//split each line by spaces
			.map(line->line.trim().split("\\s+"))
			//create a document from each line
			.map(splitLine->new Document(
					//first element in the array is the document id
					Integer.parseInt(splitLine[0]),
					Arrays.stream(splitLine)
					.skip(1)
					//split each element (of the form tid:weight) to get the key-value pair
					.map(splitToken->splitToken.trim().split(":"))
					//collect entries as a map of <tid,weight> 
					.collect(Collectors.toConcurrentMap(d->Integer.parseInt(d[0]),d->Double.parseDouble(d[1])))))
					//collect all documents as a list of document
					.collect(Collectors.toConcurrentMap(k->k.getId(),v->v));
		return documents;
	}
	
	
	public static ConcurrentMap<Integer,Document> loadDocumentsFromHDFS(String path, Configuration conf) throws IOException{
		Path docPath = new Path(path);
		ConcurrentMap<Integer,Document> documents=null;
		try (FileSystem fs =FileSystem.get(conf);
			FSDataInputStream in = fs.open(docPath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));) {
			documents=reader.lines()
					.parallel()
					//split each line by spaces
					.map(line->line.trim().split("\\s+"))
					//create a document from each line
					.map(splitLine->new Document(
							//first element in the array is the document id
							Integer.parseInt(splitLine[0]),
							Arrays.stream(splitLine)
							.skip(1)
							//split each element (of the form tid:weight) to get the key-value pair
							.map(splitToken->splitToken.trim().split(":"))
							//collect entries as a map of <tid,weight> 
							.collect(Collectors.toConcurrentMap(d->Integer.parseInt(d[0]),d->Double.parseDouble(d[1])))))
							//collect all documents as a map of document
							.collect(Collectors.toConcurrentMap(k->k.getId(),v->v));
		}
		
		
		return documents;
	}
	
	 static void writeOutputToHadoopDir(String content,String hadoopOutputDir,Configuration conf) throws IOException{
		 FileSystem fs =FileSystem.get(conf);
		 System.out.println("Writing output file");
		 Path output=new Path(hadoopOutputDir,"result");
		 FSDataOutputStream outHDFS = fs.create(output,true);
		 outHDFS.write(content.toString().getBytes());
		 outHDFS.flush();
		 outHDFS.close();
		 System.out.println("Completed writing output file");
	}
	public static ConcurrentMap<Integer,Set<String>> loadCategoriesFromHDFS(Path categoryPath, Configuration conf) throws IOException{
		final Function<Matcher,String> baseCategoryExtractor = m -> m.group("baseCategory");
		final Function<Matcher,Integer> docIDExtractor = m -> Integer.parseInt(m.group("docid"));
		Pattern pattern=Pattern.compile("^(?<category>(?<baseCategory>\\w{1})(\\w+))\\s(?<docid>\\d+)\\s1$");
		ConcurrentMap<Integer,Set<String>> categories=null;
		try (FileSystem fs =FileSystem.get(conf);
				FSDataInputStream in = fs.open(categoryPath);
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));) {
			categories=reader.lines()
					.parallel()
					//split each line by spaces
					.map(s->{
						Matcher matcher=pattern.matcher(s);
						matcher.find();
						return matcher;})
					.collect(Collectors.groupingByConcurrent(docIDExtractor,Collectors.mapping(baseCategoryExtractor,Collectors.toSet())));
		}
		return categories;
	}


	public static List<Document> getBatch(Map<Integer,Document> documents,int batchSize){
		//changeit to iterate entries and choose random element
			List<Document> docList=new ArrayList<>(documents.values());
			return IntStream
					//no. of centroids to generate
					.range(0, batchSize)
					.parallel()
					//select random documents
					.mapToObj(in->getRandomDocument(documents))
					//collect all documents as a list of document
					.collect(Collectors.toList());
		}
	
	public static List<List<Double>> loadPoints(String file, Configuration conf)
			throws Exception {
		List<List<Double>> points = new LinkedList<>();
		Path pointFilePath = new Path(file);
		FileSystem fs = pointFilePath.getFileSystem(conf);
		FSDataInputStream in = fs.open(pointFilePath);
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(
				in));) {
			reader.lines().map(line -> line.trim().split(" "))
					.forEach(coordinates -> {
						List<Double> point = new LinkedList<>();
						for (String coordinate : coordinates) {
							point.add(Double.parseDouble(coordinate));
						}
						points.add(point);
					});
		}
		return points;
	}
	

	public static ConcurrentMap<Integer,Document> getRandomCentroids(int centroids,Map<Integer,Document> documents)
	{
		return
		IntStream
		//no. of centroids to generate
		.range(0, centroids)
		.parallel()
		//select a random document and make a copy of it
		.mapToObj(in->getRandomDocument(documents))
		//collect all documents as a list of document
		.collect(Collectors.toConcurrentMap(k->k.getId(),v->v));
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


public static ConcurrentMap<Integer,Set<String>> loadCategories(String categoryPath) throws IOException{
	final Function<Matcher,String> baseCategoryExtractor = m -> m.group("baseCategory");
	final Function<Matcher,Integer> docIDExtractor = m -> Integer.parseInt(m.group("docid"));
	
	Pattern pattern=Pattern.compile("^(?<category>(?<baseCategory>\\w{1})(\\w+))\\s(?<docid>\\d+)\\s1$");
	ConcurrentMap<Integer,Set<String>> categories=Files.lines(Paths.get(categoryPath))
		.parallel()
		//split each line by spaces
		.map(s->{
			Matcher matcher=pattern.matcher(s);
			matcher.find();
			return matcher;})
		.collect(Collectors.groupingByConcurrent(docIDExtractor,Collectors.mapping(baseCategoryExtractor,Collectors.toSet())));
		return categories;
}



	
	 static void generateInitialCentroids(int numCentroids, int vectorSize, Configuration configuration,
		 Path workingDir, FileSystem fs) throws IOException {
		 StringBuilder centroidsData=new StringBuilder();
		 for (int i=0;i<numCentroids;i++){
				for(int dimension=0;dimension<vectorSize;dimension++){
					centroidsData.append(Math.random()*POINT_GENERATION_RANGE);
					centroidsData.append(' ');
				}
				//centroidsData.append(Math.random()*POINT_GENERATION_RANGE);
				//if(i+vectorSize<numCentroids){
					centroidsData.append(System.lineSeparator());
				//}
			}
		 if(fs.exists(workingDir)){
			 fs.delete(workingDir,true); 
		 }
		 FSDataOutputStream outHDFS = fs.create(new Path(workingDir,"centroidData"),true);
		 outHDFS.write(centroidsData.toString().getBytes());
		 outHDFS.flush();
		 outHDFS.close();
	 }
	 /**
	  * Writes output file to local output dir
	  * @param content Content to be written to output file
	  * @param localDirOutput Dir in which output is to be written
	  * @throws IOException
	  */
	 static void writeOutputToLocalDir(String content,String localDirOutput) throws IOException{
		 System.out.println("Writing output file");
		 java.nio.file.Path outputDir=Paths.get(localDirOutput);
		 if(Files.notExists(outputDir)){
			 Files.createDirectory(outputDir);
		 }
		 LocalDateTime now = LocalDateTime.now();
		 String outputFileName="output-"+now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss", Locale.ENGLISH));
		 java.nio.file.Path outputFile=Paths.get(localDirOutput,outputFileName);
		 Files.write(outputFile, content.getBytes());
		 System.out.println("Completed writing output file");
	}
	
	 
	 static void writeCompletion(String message,FileSystem fs,String workingDir) throws IOException{
		 Path output=new Path(workingDir,"completion");
		 FSDataOutputStream outHDFS = fs.create(output,true);
		 outHDFS.write(message.getBytes());
		 outHDFS.flush();
		 outHDFS.close();
	 }
	 
	 static void writeCentroids(Table<DoubleArray> centroidTable,String outputDir, FileSystem fs) throws IOException{
		 //Path outputDirPath=new Path(outputDir);
		 Path output=new Path(outputDir,"centroids");
//		 if(fs.exists(outputDirPath)){
//				fs.delete(outputDirPath,true);
//		 }
//		 fs.mkdirs(outputDirPath);
		 StringBuilder centroids=new StringBuilder();
		 for(Partition<DoubleArray> partition:centroidTable.getPartitions()){
			 DoubleArray array = partition.get();
			 double[] backingArr = (double[]) array.get();
			 for(int i=0;i<array.size()-2;i++){
				 centroids.append(backingArr[i]);
				 centroids.append(' ');
			 }
			 centroids.append(backingArr[array.size()-2]);
			 centroids.append(System.lineSeparator());
		 }
		 FSDataOutputStream outHDFS = fs.create(output,true);
		 outHDFS.write(centroids.toString().getBytes());
		 outHDFS.flush();
		 outHDFS.close();
		 
	 }
}