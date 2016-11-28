package edu.iu.km;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.iu.fileformat.MultiFileInputFormat;

public class KmeansMapCollective  extends Configured implements Tool {
	private static final int NUMBER_OF_MAPPERS=2;
	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KmeansMapCollective(), argv);
		System.exit(res);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		//keep this unchanged.
		if (args.length < 6) {
			System.err.println("KmeansMapCollective <batchSizeInPercent> <num of Centroids> <number of iteration> <workDir> <localDir for data> <localDir for application output>");			
			ToolRunner.printGenericCommandUsage(System.err);
				return -1;
		}
		double batchSizeInPercent=Double.parseDouble(args[0]);
		int numOfCentroids=Integer.parseInt(args[1]);
		int numMapTasks=NUMBER_OF_MAPPERS;
		int iterations=Integer.parseInt(args[2]);
		Path workingDir=new Path(args[3]);
		String localDirData=args[4];
		String localDirOutput=args[5];
		String docVectorFilesDir=localDirData+File.separator+"documents";
		String categoriesDir=localDirData+File.separator+"categories";
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);
		Path data=new Path (workingDir,"data");
		Path documents=new Path (data,"documents");
		Path categories=new Path(data,"categories");
		Utils.copyDocuments(fs, docVectorFilesDir, documents);
		Utils.copyCategories(fs,categoriesDir,categories);
		Job job=Job.getInstance(conf, "mini-batch-kmeans");
		FileInputFormat.setInputPaths(job, new Path[] { documents });
		Path hadoopOutput=new Path (workingDir,"hadoopOutput1");
		Path appOutput=new Path (workingDir,"appOutput1");
		if(fs.exists(hadoopOutput)){
			fs.delete(hadoopOutput,true);
		}

		if(fs.exists(appOutput)){
			fs.delete(appOutput,true);
		}
		fs.mkdirs(appOutput);
		FileOutputFormat.setOutputPath(job, hadoopOutput);
	    job.setInputFormatClass(MultiFileInputFormat.class);
	    job.setJarByClass(KmeansMapCollective.class);
	    job.setMapperClass(KmeansMapper.class);
	    job.setNumReduceTasks(0);
	    JobConf jobConf = (JobConf)job.getConfiguration();
	    jobConf.set("mapreduce.framework.name", "map-collective");
	    jobConf.setNumMapTasks(numMapTasks);
	    jobConf.setInt("mapreduce.job.max.split.locations", 10000);
	    Configuration jobConfig = job.getConfiguration();
	    jobConfig.setDouble("batchSizeInPercent",batchSizeInPercent);
	    jobConfig.setInt("numOfCentroids",numOfCentroids);
	    jobConfig.set("outputFilePath",appOutput.toString());
	    jobConfig.setInt("iterations", iterations);
	    jobConfig.set("workingDir", args[3]);
	    jobConfig.set("localDirOutput",localDirOutput);
	    jobConfig.setInt("numMapTasks", numMapTasks);
	    boolean complete = job.waitForCompletion(true);
	    if(complete){
	    	System.out.println("Job completed");
	    }
	    else{
	    	System.out.println("Job not completed");
	    }
	    return 0;
	    
	}
	
	
	
}