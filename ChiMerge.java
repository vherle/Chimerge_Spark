  
package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;


/**
 *  
 *  1. Unique attribute value
 *  2. Sort by attribute value
 *  3. partition with redundancy.
 *  4. combine adjacent blocks and compute ChiSquare
 *  5. Take global minimum.
 *  6. merge ChiSquareUnits
 *  7. Merge adjacent blocks(by partitioning) until they don't further merge. 
 *  8. Then back to Step 2:
 *
 */
@SuppressWarnings({ "serial" })
public class ChiMerge implements Serializable {
	
	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.OFF);
	    SparkConf sparkConf = new SparkConf().setAppName("Local");
	    sparkConf.setMaster("local");
	    sparkConf.set("spark.executor.memory", "1g");
	    sparkConf.set("spark.driver.memory", "1g");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    
	    long startTime = System.currentTimeMillis();
	    // Read the data from the file.
	    JavaRDD<String> stringRdd = jsc.textFile("./testData/Iris.txt", 3);
	    
	    //Step: Map raw line read from file to a IrisRecord.
	    JavaRDD<IrisRecord> data = stringRdd.map(new Function<String, IrisRecord>() {
			public IrisRecord call(String v1) throws Exception {
				return new IrisRecord(v1);
			}
		});
	    
	    // Create a JavaPairRDD with attribute value, record itself.
	    JavaPairRDD<Double, IrisRecord> mapToPair = data.mapToPair(new PairFunction<IrisRecord, Double, IrisRecord>() {
			public Tuple2<Double, IrisRecord> call(IrisRecord t) throws Exception {
				return new Tuple2<Double, IrisRecord>(t.getSepalLength(), t);
			}
		});
	    
	    //Group by key to pull all records with same value together.
	    JavaPairRDD<Double, Iterable<IrisRecord>> groupByKey = mapToPair.groupByKey();
	    
	    //Now lets create a Blockie which contains value and all its records which have that value. We need this for computing
	    // Chisquare.
	    JavaPairRDD<Double, Blockie> blocks = groupByKey.mapValues(new Function<Iterable<IrisRecord>, Blockie>() {
			public Blockie call(Iterable<IrisRecord> v1) throws Exception {
				List<IrisRecord> records = Lists.newArrayList(v1);
				records.get(0).getSepalLength();
				return new Blockie(records, records.get(0).getSepalLength());
			}
		});
	    
	    BigDecimal min = BigDecimal.valueOf(Double.MIN_VALUE);
		BigDecimal threshold = BigDecimal.valueOf(4.605);
		JavaRDD<Blockie> sourceRdd = null;
		while(min.compareTo(threshold) < 0) {
		
		/*******************/
//		int index = 0;
//		while(index < 1) {
//	    	index++;
	    /*******************/
	    	
		    // Lets sort the blocks by attribute value.
		    JavaPairRDD<Double, Blockie> sortedBlocksRdd = blocks.sortByKey(true);
		    
		    //Map Partitions With index
		    JavaRDD<Tuple2<Integer, Tuple2<Double, Blockie>>> mapPartitionsWithIndex = sortedBlocksRdd.
		    		mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Double, Blockie>>, Iterator<Tuple2<Integer, Tuple2<Double, Blockie>>>>() {
	
				public Iterator<Tuple2<Integer, Tuple2<Double, Blockie>>> call(Integer v1,
						Iterator<Tuple2<Double, Blockie>> v2) throws Exception {
					
					List<Tuple2<Integer, Tuple2<Double, Blockie>>> list = Lists.newArrayList();
					while(v2.hasNext()) {
						Tuple2<Double,Blockie> next = v2.next();
						list.add(new Tuple2<Integer, Tuple2<Double,Blockie>>(v1, next));
					}
					if(!list.isEmpty() && v1 > 0) {
						// This step is the one which takes the first element from this
						// partition and puts it in the previous partition. 
						// Hence maintaining the data continuity even with partitions.
						Tuple2<Double, Blockie> firstRecord = list.get(0)._2();
						list.add(new Tuple2<Integer, Tuple2<Double,Blockie>>(v1 - 1, firstRecord));
					}
					return list.iterator();
				}
			}, false);
		    
		    // We have not yet partitioned the data. We have just assigned each blockie to a partition number in the previous step.
		    // The below step creates the partitions based on the partition number we assigned previously.
		    JavaPairRDD<Integer, Tuple2<Double, Blockie>> mappedPartitions = mapPartitionsWithIndex
		    		.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Double, Blockie>>, Integer, Tuple2<Double, Blockie>>() {
				public Tuple2<Integer, Tuple2<Double, Blockie>> call(Tuple2<Integer, Tuple2<Double, Blockie>> t) throws Exception {
					return t;
				}
			})
			.partitionBy(
				new SimplePartitioner(sortedBlocksRdd.partitions().size())
			);
		    
		    // now create ChiSqUnit and compute chiSquare.
		    JavaRDD<ChisquareUnit> chiSquaredRdd = mappedPartitions.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer,Tuple2<Double,Blockie>>>, ChisquareUnit>() {
	
				public Iterable<ChisquareUnit> call(Iterator<Tuple2<Integer, Tuple2<Double, Blockie>>> t)
						throws Exception {
					List<Tuple2<Double, Blockie>> rawList = Lists.newArrayList();
					List<ChisquareUnit> returnList = Lists.newArrayList();
					
					while(t.hasNext()) {
						rawList.add(t.next()._2());
					}
					// Lets sort the data again. This is because when the data was added to the previous partition,
					// the sorted arrangement may have been lost. Since the partition fits into the worker's memory
					// we don't have to create a RDD to sort the partition data.
					Collections.sort(rawList, new Comparator<Tuple2<Double, Blockie>>() {
	
						public int compare(Tuple2<Double, Blockie> o1, Tuple2<Double, Blockie> o2) {
							return BigDecimal.valueOf(o1._1()).compareTo(BigDecimal.valueOf(o2._1()));
						}
					});
	
					for(int i = 0; i < rawList.size() - 1; i++) {
						ChisquareUnit unit = new ChisquareUnit(rawList.get(i)._2(), rawList.get(i + 1)._2());
						// Compute the ChiSquare.
						unit.computeChiSquare();
						returnList.add(unit);
					}
					return returnList;
				}
			});
		    
		    // Compute the Global minimum of the Chisquare and then merge the Blocks with the minimum value. 
		    min = BigDecimal.valueOf(chiSquaredRdd.min(new Sorter()).getChiSquareValue());
		    final Double minimum = min.doubleValue();
		    
		    JavaRDD<Blockie> cm = chiSquaredRdd.mapPartitions(new FlatMapFunction<Iterator<ChisquareUnit>, Blockie>() {
	
				public Iterable<Blockie> call(Iterator<ChisquareUnit> t) throws Exception {
					List<Blockie> blocks = Lists.newArrayList();
					while(t.hasNext()) {
						ChisquareUnit chUnit = t.next();
						if (BigDecimal.valueOf(chUnit.getChiSquareValue()).compareTo(BigDecimal.valueOf(minimum)) == 0) {
							blocks.add(chUnit.getBlock1().merge(chUnit.getBlock2()));
						} else {
							blocks.add(chUnit.getBlock1());
							blocks.add(chUnit.getBlock2());
						}
					}
					return blocks;
				}
			});
		    
		    // ******* begin loop : Combine *******
		    sourceRdd = cm;
		    int i = 0;
		    
		    do {
		    	i++;
		    	sourceRdd = sourceRdd.mapPartitions(new FlatMapFunction<Iterator<Blockie>, Blockie>() {
	
					public Iterable<Blockie> call(Iterator<Blockie> t) throws Exception {
						List<Blockie> list = Lists.newArrayList();
						List<Blockie> mergedList = Lists.newArrayList();
						
						while (t.hasNext()) {
							list.add(t.next());
						}
						
						if(list.isEmpty()) {
							return mergedList;
						}
	
						Collections.sort(list, new Comparator<Blockie>() {
							public int compare(Blockie o1, Blockie o2) {
								return o1.getFingerPrint().compareTo(o2.getFingerPrint());
							}
						});
						
						Blockie current = list.get(0);
						int i = 1;
						while (i < list.size()) {
							Blockie next = list.get(i);
							if (current.contains(next)) {
								// Nothing.
							} else if (next.contains(current)) {
								current = next;
							} else if (current.overlaps(next)) {
								current = current.merge(next);
							} else {
								mergedList.add(current);
								current = next;
							}
							i++;
						}
						if (current != null) {
							mergedList.add(current);
						}
						return mergedList;
					}
				});
		    	sourceRdd = sourceRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Blockie>, BigDecimal, Blockie>() {
					public Iterable<Tuple2<BigDecimal, Blockie>> call(Iterator<Blockie> t) throws Exception {
						List<Tuple2<BigDecimal, Blockie>> list = Lists.newArrayList();
						while(t.hasNext()) {
							Blockie b = t.next();
							list.add(new Tuple2<BigDecimal, Blockie>(b.getFingerPrint(), b));
						}
						return list;
					}
				})
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<BigDecimal,Blockie>>, Iterator<Tuple2<Integer, Tuple2<BigDecimal, Blockie>>>>() {
	
					public Iterator<Tuple2<Integer, Tuple2<BigDecimal, Blockie>>> call(Integer v1,
							Iterator<Tuple2<BigDecimal, Blockie>> v2) throws Exception {
						
						List<Tuple2<Integer, Tuple2<BigDecimal, Blockie>>> list = Lists.newArrayList();
						while(v2.hasNext()) {
							Tuple2<BigDecimal,Blockie> next = v2.next();
							list.add(new Tuple2<Integer, Tuple2<BigDecimal,Blockie>>(v1, next));
						}
						if(! list.isEmpty() && v1 > 0) {
							// This step is the one which takes the first element from this
							// partition and puts it in the previous partition. 
							// Hence maintaining the data continuity even with partitions.
							Tuple2<BigDecimal, Blockie> firstRecord = list.get(0)._2();
							list.add(new Tuple2<Integer, Tuple2<BigDecimal,Blockie>>(v1 - 1, firstRecord));
							while(list.remove(new Tuple2<Integer, Tuple2<BigDecimal,Blockie>>(v1, firstRecord)));
						}
						return list.iterator();
					}
				}, true)
				.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<BigDecimal,Blockie>>, Integer, Tuple2<BigDecimal, Blockie>>() {
	
					public Tuple2<Integer, Tuple2<BigDecimal, Blockie>> call(Tuple2<Integer, Tuple2<BigDecimal, Blockie>> t)
							throws Exception {
						return t;
					}
				})
				.partitionBy(new SimplePartitioner(sourceRdd.partitions().size()))
				.values()
				.map(new Function<Tuple2<BigDecimal,Blockie>, Blockie>() {
	
					public Blockie call(Tuple2<BigDecimal, Blockie> v1) throws Exception {
						return v1._2();
					}
				});
				
		    } while(i < sourceRdd.partitions().size());
		    
		    blocks = sourceRdd.mapToPair(new PairFunction<Blockie, Double, Blockie>() {

				public Tuple2<Double, Blockie> call(Blockie t) throws Exception {
					return new Tuple2<Double, Blockie>(t.getFingerPrint().doubleValue(), t);
				}
			});
		    
		    
	    } // end of big while (Threshold value)

		printBlockRanges(sourceRdd);
	    System.out.println("Time to run: " + (System.currentTimeMillis() - startTime));	    
	    jsc.stop();
	    
	}
	
	public static void printBlockRanges(JavaRDD<Blockie> bh) {
		for (Blockie b : bh.collect()) {
			System.out.println(b.getRange());
		}
	}
	
	private static class SimplePartitioner extends Partitioner {

		private int partitions;
		
		public SimplePartitioner(int num) {
			this.partitions = num;
		}
		
		@Override
		public int getPartition(Object arg0) {
			return (Integer) arg0;
		}

		@Override
		public int numPartitions() {
			return this.partitions;
		}
	}
	
	private static class Sorter implements Comparator<ChisquareUnit>, Serializable {
		public int compare(ChisquareUnit o1, ChisquareUnit o2) {
			return BigDecimal.valueOf(o1.getChiSquareValue()).compareTo(BigDecimal.valueOf(o2.getChiSquareValue()));
		}
	}
}

