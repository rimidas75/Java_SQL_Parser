package dubstep;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

class EvalLib_Impl extends Eval {

	public Map<String, Integer[]> _colmap = new HashMap<String, Integer[]>();
	public String row;
	public PrimitiveValue[] pvArr;
	public String[] rowArr;

	public EvalLib_Impl(Map<String, Integer[]> _colmap, String row) {
		super();
		this._colmap = _colmap;
		this.row = row;
		this.rowArr = (this.row).split(Pattern.quote("|"));
	}
	
	public EvalLib_Impl() {
		super();
	}

	private PrimitiveValue rowToPrimitive(String key) {
		Integer[] mapVal = _colmap.get(key);
		String val = rowArr[mapVal[0]];
		int valType = mapVal[1];
		switch (valType) {
		case 1:
			return new StringValue(val);
		case 2:
			return new LongValue(val);
		case 3:
			return new DoubleValue(val);
		case 4:
			return new DateValue(val);
		default:
			return null;
		}

	}

	@Override
	public PrimitiveValue eval(Column arg0) throws SQLException {
		return rowToPrimitive(arg0.getColumnName());
		
	}

}
