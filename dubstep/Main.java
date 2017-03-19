package dubstep;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.Table;
import dubstep.EvalLib_Impl;

public class Main {
	static Map<String, List<ColumnDefinition>> TableMap = new HashMap<String, List<ColumnDefinition>>();

	public static void main(String[] args) throws java.sql.SQLException {
		// SELECT COUNT(*) FROM data WHERE D >19;
		// SELECT SUM(A+D) FROM data WHERE D > 19;
		// CREATE TABLE data(A int, B string, C string, D int);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String strline = null;
		System.out.print("$> ");
		
		try {
			while ((strline = br.readLine()) != null) {
				StringReader input = new StringReader(strline);
				CCJSqlParser parser = new CCJSqlParser(input);
				StringBuilder s = new StringBuilder();

				try {

					Statement query = parser.Statement();
					if (query instanceof Select) {
						Select select = (Select) query;

						SelectBody sb = select.getSelectBody();

						if (sb instanceof PlainSelect) {
							PlainSelect ps = (PlainSelect) sb;

							FromItem fi = ps.getFromItem();
							Expression wi = ps.getWhere();
							List<SelectItem> so = ps.getSelectItems();
							String tableName = null;
							File f = null;

							if (fi instanceof Table) {
								Table tb = (Table) fi;

								tableName = tb.getName();
								BaseIterator bi = null;
								ArrayList<PrimitiveValue> agg_res = new ArrayList<PrimitiveValue>();
								int cnt = 0;
								int siIterCnt = 0;
								Map<Integer, Aggregators> agg_map = new HashMap<Integer, Aggregators>();

								f = new File("data/" + tableName + ".csv");
                                Iterator<ColumnDefinition> colIterator = TableMap.get(tableName).iterator();
								bi = new BaseIterator(f, colIterator);
								FilterIterator fil = new FilterIterator(bi, wi);
								while (fil.hasNext()) {
									String row = fil.next();
									
									siIterCnt = 0;

									if (row != null) {
										Function fn;
										Iterator<SelectItem> selectItemIterator = so.iterator();
										EvalLib_Impl eval = new EvalLib_Impl(bi.colMap, row);
										
										while (selectItemIterator.hasNext()) {
											SelectExpressionItem sIter = (SelectExpressionItem) selectItemIterator.next();
											Expression selectExp = sIter.getExpression();
											if (selectExp instanceof Function) {
												fn = (Function)selectExp;
												if(fn.getParameters() != null) {
													selectExp = fn.getParameters().getExpressions().get(0);
												} else {
													selectExp = (Expression) new StringValue("1");
												}
												PrimitiveValue pvResult = eval.eval(selectExp);
												if(cnt == 0) {
													agg_map.put(siIterCnt, new Aggregators());
													if(fn.getName().equalsIgnoreCase("SUM")) {
														agg_res.add(agg_map.get(siIterCnt).gsum(pvResult));
													} else if (fn.getName().equalsIgnoreCase("MIN")) {
														agg_res.add(agg_map.get(siIterCnt).gmin(pvResult));
													} else if (fn.getName().equalsIgnoreCase("MAX")) {
														agg_res.add(agg_map.get(siIterCnt).gmax(pvResult));
													} else if (fn.getName().equalsIgnoreCase("COUNT")) {
														agg_res.add(agg_map.get(siIterCnt).gcnt(pvResult));
													}
													
												} else {
													if(fn.getName().equalsIgnoreCase("SUM")) {
														agg_res.set(siIterCnt, agg_map.get(siIterCnt).gsum(pvResult));
													} else if (fn.getName().equalsIgnoreCase("MIN")) {
														agg_res.set(siIterCnt, agg_map.get(siIterCnt).gmin(pvResult));
													} else if (fn.getName().equalsIgnoreCase("MAX")) {
														agg_res.set(siIterCnt, agg_map.get(siIterCnt).gmax(pvResult));
													} else if (fn.getName().equalsIgnoreCase("COUNT")) {
														agg_res.set(siIterCnt, agg_map.get(siIterCnt).gcnt(pvResult));
													}
												}
												
											} else {

												PrimitiveValue pvResult = eval.eval(selectExp);
												s.append(pvResult.toRawString()).append("|");
												
											}
											
											siIterCnt++;

										}
										cnt++;
										if(s.length() > 0) {
											s = new StringBuilder(s.substring(0, s.length()-1));
											s.append("\n");
										}

									}
																									
								}

								if(s.length() > 0) {
									s = new StringBuilder(s.substring(0, s.length()-1));
									
								}
								
								int j=0;
								if(agg_res.size() > 0) {
									for(PrimitiveValue v : agg_res) {
										j++;
										System.out.print(v.toRawString());
										if(!(j == agg_res.size()))
											System.out.print("|");
									}
									System.out.print("\n");
								} else {
									System.out.println(s);
								}
								
							}
						}
					} else if (query instanceof CreateTable) {
						CreateTable ct = (CreateTable) query;
						Table t = ct.getTable();
						TableMap.put(t.getName(), ct.getColumnDefinitions());
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
				
				System.out.print("$> ");
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
}
