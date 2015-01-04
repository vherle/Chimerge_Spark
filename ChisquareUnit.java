package SparkTesting;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.stat.Statistics;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

/**
 *
 */
public class ChisquareUnit implements Serializable {

	private static final long serialVersionUID = 1L;

	private Blockie block1;

	private Blockie block2;

	private Double chiSquareValue;

	public ChisquareUnit(Blockie block1, Blockie block2) {
		this.block1 = block1;
		this.block2 = block2;
		
	}
	
	public void computeChiSquare() {
		this.chiSquareValue = Statistics.chiSqTest(mergeMatricies(computeSingleRowMatrix(block1), computeSingleRowMatrix(block2))).statistic();
	}

	public Double getChiSquareValue() {
		return chiSquareValue;
	}

	public Blockie getBlock1() {
		return block1;
	}

	public Blockie getBlock2() {
		return block2;
	}

	private Table<String, Double, Integer> computeSingleRowMatrix(Blockie block) {
		List<IrisRecord> records = block.getAllRecords();
		Set<Double> uniqueClassLabel = Sets.newHashSet();
		for(IrisRecord i: records){
			uniqueClassLabel.add(i.getSpecies());
		}
		
		Table<String, Double, Integer> table = HashBasedTable.create();
		for(Double d: uniqueClassLabel) {
			table.put("Row", d , 0);
		}
		
		for (IrisRecord iris : records) {
			int count = table.get("Row", iris.getSpecies());
			table.put("Row", iris.getSpecies(), ++count);
		}
		
		return table;
	}
	
	private Matrix mergeMatricies(Table<String, Double, Integer> table1, Table<String, Double, Integer> table2) {
		
		Table<String, Double, Integer> mergedTable = HashBasedTable.create();
		Set<Double> mergedCols = Sets.newHashSet();
		mergedCols.addAll(table1.columnKeySet());
		mergedCols.addAll(table2.columnKeySet());		
		for(Double d: mergedCols) {
			mergedTable.put("Row-1", d, 0);
			mergedTable.put("Row-2", d, 0);
		}
		for(Double col: table1.columnKeySet()) {
			mergedTable.put("Row-1", col, table1.get("Row", col));
		}
		for(Double col: table2.columnKeySet()) {
			mergedTable.put("Row-2", col, table2.get("Row", col));
		}

		return new DenseMatrix(2, mergedCols.size(), tableToArray(mergedTable));
	}
	
	private <K> double[] tableToArray(Table<K, Double, Integer> table) {
		double[] array = new double[table.size()];
		int index = 0;
		Set<Double> cols = table.columnKeySet();
		for(Double col: cols){
			Map<K, Integer> rows = table.column(col);
			for(Entry<K, Integer> entry: rows.entrySet()) {
				array[index++] = entry.getValue();
			}
		}
		return array;
	}
	
	@Override
	public String toString(){
		return "ChisquareUnit " + block1 + " " + block2 ;
	}
}
