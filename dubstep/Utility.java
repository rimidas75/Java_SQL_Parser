package dubstep;

public class Utility {

	public static int getDataType(String datatype)
	{
		switch(datatype.toLowerCase())
		{
		case "string" : return 1;
		case "varchar": return 1;
		case "char": return 1;
		case "int": return 2;
		case "decimal": return 3;
		case "date": return 4;
			
		}
		return 1;
		
	}
}
