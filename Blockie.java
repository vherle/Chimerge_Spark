package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
public class Blockie implements Serializable {
	
	Map<Double, List<IrisRecord>> map = Maps.newHashMap();

	public Blockie(List<IrisRecord> records, Double id) {
		this.map.put(id, records);
	}

	private Blockie(Map<Double, List<IrisRecord>> map) {
		this.map = map;
	}
	
	public List<IrisRecord> getRecordsForId(Double id) {
		return map.get(id);
	}

	public Set<Double> getIds() {
		return map.keySet();
	}
	
	@Override
	public String toString() {
		return "Blockie [values = " + getIds() + "]";
	}

	public boolean contains(Blockie b) {
		return this.map.keySet().containsAll(b.getMap().keySet());
	}

	public Map<Double, List<IrisRecord>> getMap() {
		return map;
	}
	
	public BigDecimal getFingerPrint(){
		BigDecimal fingerPrint = BigDecimal.ZERO;
		for(Double d: getIds()) {
			fingerPrint = fingerPrint.add(BigDecimal.valueOf(d));
		}
		return fingerPrint.divide(BigDecimal.valueOf(getIds().size()), RoundingMode.HALF_EVEN);
	}
	
	public Blockie merge(Blockie b) {
		Map<Double, List<IrisRecord>> map = Maps.newHashMap();
		map.putAll(this.map);
		map.putAll(b.getMap());
		return new Blockie(map);
	}
	
	public boolean overlaps(Blockie b) {
		return !Sets.intersection(this.getIds(), b.getIds()).isEmpty();
	}
	
	public List<IrisRecord> getAllRecords() {
		List<IrisRecord> records = Lists.newArrayList();
		for(List<IrisRecord> list: map.values()){
			records.addAll(list);
		}
		return records;
	}
	
	public String getRange() {
		List<Double> keys = Lists.newArrayList(map.keySet());
		Collections.sort(keys);
		return "[" + keys.get(0) + " - " + keys.get(keys.size() - 1) + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((map == null) ? 0 : map.hashCode());
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
		Blockie other = (Blockie) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}
}
