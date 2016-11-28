package edu.iu.fileformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import edu.iu.harp.resource.Writable;

public class Document extends Writable implements Serializable, Cloneable {
	private static final long serialVersionUID = -7694155197374922059L;
	private static final Double DEFAULT_VECTOR_VALUE = 0.0;
	private int id;
	private ConcurrentMap<Integer, Double> featureVector;
	private AtomicInteger pointsAssociatedCount;

	public Document(final int id, final ConcurrentMap<Integer, Double> vector) {
		this(id,vector,new AtomicInteger());
	}
	public Document(final int id, final ConcurrentMap<Integer, Double> vector,AtomicInteger pointsAssociatedCount) {
		super();
		this.id = id;
		this.featureVector = vector;
		this.pointsAssociatedCount=pointsAssociatedCount;
	}
	public Document(Document doc) {
		this(doc.getId(), new ConcurrentHashMap<Integer, Double>(doc.featureVector));
	}
	public Document(){
		
	}

	public int getId() {
		return id;
	}

	public AtomicInteger getPointsAssociatedCount() {
		return pointsAssociatedCount;
	}
	public void setPointsAssociatedCount(AtomicInteger pointsAssociatedCount) {
		this.pointsAssociatedCount = pointsAssociatedCount;
	}
	public Double setFeatureValue(Integer featureID, Double featureValue) {
		return featureVector.put(featureID, featureValue);
	}

	public Double getFeatureValue(Integer featureID) {
		return featureVector.getOrDefault(featureID, DEFAULT_VECTOR_VALUE);
	}

	public void updateFeatureValues(
			BiFunction<Integer, Double, Double> reMappingFunction) {
		featureVector.replaceAll(reMappingFunction);
	}	

	public double calculateCosineSimilarity(Document other) {
		
		double dotProduct = featureVector.entrySet()
				.parallelStream()
				// .stream()
				.mapToDouble(
						entry -> entry.getValue()
								* other.getFeatureValue(entry.getKey())).sum();
		double d1Norm = Math.sqrt(featureVector.values().parallelStream()
				.map(x -> Math.pow(x, 2))
				// .stream()
				.reduce(0.0, Double::sum));
		double d2Norm = Math.sqrt(featureVector.values().parallelStream()
				.map(x -> Math.pow(x, 2))
				// .stream()
				.reduce(0.0, Double::sum));
		return (dotProduct / (d1Norm * d2Norm));
	}

	@Override
	public void clear() {
		featureVector=null;
		pointsAssociatedCount=null;
	}

	@Override
	public int getNumWriteBytes() {
		/*
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try(ObjectOutputStream out = new ObjectOutputStream(baos);
				) {
			System.out.println("baos size: initial"+baos.size());
			out.writeObject(this);
			System.out.println("object written to baos");
			out.flush();
			System.out.println("baos size: after write"+baos.size());
		} catch (IOException ioEx) {
			System.err.println("baos size: in exception"+baos.size());
			System.err.println("****Failed to get no. of bytes for Document object****");
			ioEx.printStackTrace();
			System.err.println("Completed printing getNumWriteBytes exception trace");
		}
		return baos.size()+4;
		*/
		
		return 8+featureVector.size()*12+4;
	}

	public static void main(String... args){
		Document d=new Document(0, new ConcurrentHashMap<Integer, Double>());
		System.out.println(d.getNumWriteBytes());		
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		/*
		int objSize = in.readInt();
		byte[] data = new byte[objSize];
		in.readFully(data, 4, objSize);
		ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
		ObjectInputStream oins = new ObjectInputStream(inputStream);
		try {
			Document document = (Document) oins.readObject();
			this.featureVector = document.featureVector;
			this.id = document.id;
		} catch (ClassNotFoundException e) {
			System.err
					.println("Failed to read document from ByteArrayInputStream");
			e.printStackTrace();
		}
		*/
		//read map size
		int mapSize=in.readInt();
		//read document ID
		this.id=in.readInt();
		featureVector=new ConcurrentHashMap<>();
		//populate the featureVector
		for(int i=0;i<mapSize;i++){
			int featureID=in.readInt();
			double featureVal=in.readDouble();
			featureVector.put(featureID, featureVal);
		}
		//read pointsAssociatedCount
		pointsAssociatedCount=new AtomicInteger(in.readInt());
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		/*
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try(ObjectOutputStream objOut = new ObjectOutputStream(baos);) {
			objOut.writeObject(this);
			objOut.flush();
			byte[] data = baos.toByteArray();
			output.writeInt(data.length);
			output.write(data, 0, data.length);
		} catch (IOException ioEx) {
			System.err
					.println("Failed to write bytes of Document object");
			ioEx.printStackTrace();
		}
*/
		//size of map serialized
		output.writeInt(featureVector.size());
		//write document id
		output.writeInt(id);
		//map serialization
		for(Map.Entry<Integer,Double> entry:featureVector.entrySet()){
			output.writeInt(entry.getKey());
			output.writeDouble(entry.getValue());
		}
		//points associated
		output.writeInt(pointsAssociatedCount.get());
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Document other = (Document) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Document [id=" + id + ", vector=" + featureVector + "]";
	}
}