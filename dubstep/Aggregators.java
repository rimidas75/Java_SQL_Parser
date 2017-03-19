package dubstep;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;

public class Aggregators {
	
	private PrimitiveValue vsum;
	
	private PrimitiveValue vmax;
	
	private PrimitiveValue vmin;
	
	private PrimitiveValue vcnt;
	
	EvalLib_Impl evl;
	
	public Aggregators() {
		this.evl = new EvalLib_Impl();
		
		this.vsum = new LongValue(0);
		
		this.vmax = new DoubleValue(Double.MIN_VALUE);
		
		this.vmin = new DoubleValue(Double.MAX_VALUE);
		
		this.vcnt = new LongValue(0);
	}
	
	public PrimitiveValue gsum(PrimitiveValue val) throws SQLException {
		this.vsum = evl.eval(new Addition(val, this.vsum));
		return this.vsum;
	}
	
	public PrimitiveValue gmax(PrimitiveValue val) throws SQLException {
		if(evl.eval(new GreaterThan(val, this.vmax)).toBool()) {
			this.vmax = val;
		}
		return this.vmax;
	}
	
	public PrimitiveValue gmin(PrimitiveValue val) throws SQLException {
		if(evl.eval(new GreaterThan(this.vmin, val)).toBool()) {
			this.vmin = val;
		}
		return this.vmin;
	}
	
	public PrimitiveValue gcnt(PrimitiveValue val) throws SQLException {
		this.vcnt = evl.eval(new Addition(new LongValue(1), vcnt));
		return new LongValue(vcnt.toLong());
	}

}
