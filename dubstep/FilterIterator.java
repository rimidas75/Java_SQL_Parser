package dubstep;


import java.sql.SQLException;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

class FilterIterator implements Iterator<String> {
	public BaseIterator bi;
	public Expression exp;
	EvalLib_Impl eval;
	
	public FilterIterator(BaseIterator basetable, Expression exp) {
		super();
		this.bi = basetable;
		this.exp = exp;	
		
	}

	@Override
	public boolean hasNext() {

		return bi.hasNext();
		
	}

	@Override
	public String next() {
		if(this.bi.hasNext())
		{
			String row = this.bi.next();
			try {
				eval = new EvalLib_Impl((this.bi).colMap, row);
				if(null!= this.exp && !this.exp.equals(null))
				{	
				PrimitiveValue result = eval.eval(this.exp);
				if(result.toBool()) {
					return row;
				}
				else
					return null;
				}
				else
					return row;
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
		return null;
	}
	
}
