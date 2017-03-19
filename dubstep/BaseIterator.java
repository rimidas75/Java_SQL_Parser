package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

class BaseIterator implements Iterator<String> {
	
	public FileReader fr;
	int[] colAtt = new int[2];
	
	public Map<String, Integer[]> colMap;
	
	public BufferedReader br;
	
	public BaseIterator(File f,Iterator<ColumnDefinition> colIterator) throws FileNotFoundException {
		super();
		
		fr = new FileReader(f);
		br = new BufferedReader(fr);
		colMap = new HashMap<String, Integer[]>();

		int c = 0;
		while(colIterator.hasNext())
		{
			ColumnDefinition colDef = colIterator.next();	
			int dataType = Utility.getDataType(colDef.getColDataType().getDataType());
			Integer[] col = {c,dataType};
			colMap.put(colDef.getColumnName(), col);
			c++;
			
			
		}

	}

	@Override
	public boolean hasNext() {
		try {
			if(this.br.ready()) {
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public String next() {
		try {
			return br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
}
