package com.acme.hive;

/*
 * 1) create table DUAL (in database default)
 * $ vi dual.txt
 * X
 * :wq
 * 
 * $ hdfs dfs -mkdir dual
 * $ hdfs dfs -put dual.txt dual
 * 
 * $ hive
 * hive> CREATE EXTERNAL TABLE dual (dummy STRING) STORED AS TEXTFILE LOCATION '/user/cloudera/dual';
 * hive> exit;
 * $
 * 
 * 2) test
 hive
 ADD JAR git/hiveudf/hiveudf/target/hiveudf-0.0.1-SNAPSHOT.jar;
 CREATE TEMPORARY FUNCTION myNVL AS 'com.acme.hive.MyNVL';
 DESCRIBE FUNCTION MyNVL;
 DESCRIBE FUNCTION EXTENDED MyNVL;
 SELECT myNVL(1,2) as a FROM dual;
 SELECT myNVL(null,2) as b FROM dual;
SELECT myNVL('c','string') as c FROM dual;
SELECT myNVL(null,'string') as d FROM dual;
SELECT myNVL(from_utc_timestamp('1972-08-29 00:00:00','CET'),from_utc_timestamp(from_unixtime(unix_timestamp()),'CET')) as e FROM dual;
SELECT myNVL(null,from_unixtime(unix_timestamp())) as f FROM dual;
SELECT myNVL(1,2.0) as g FROM dual;
SELECT myNVL(1,'conversion') as h FROM dual;

 SELECT myNVL(1,2) as a, myNVL(null,2) as b, 
 myNVL('c','string') as c, myNVL(null,'string') as d,
 myNVL(from_utc_timestamp('1972-08-29 00:00:00','CET'),from_utc_timestamp(from_unixtime(unix_timestamp()),'CET')) as e
 myNVL(null,from_unixtime(unix_timestamp())) as f,
 myNVL(1,2.0) as g, myNVL(1,'conversion') as h 
 FROM dual;
 Fails:
 SELECT myNVL(1) FROM dual;
 SELECT myNVL(1,2,3) FROM dual;

 */
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException; 
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "nvl", value = "_FUNC_ .. return the second argument, if the first is null,"
		+ " otherwise it returns the first argument", extended = "_FUNC_(arg1, arg2)\n"
		+ "Examples:\n"
		+ "SELECT _FUNC_(1,2) FROM src; .. returns 1\n"
		+ "SELECT _FUNC_(null,2) FROM src; .. returns 2\n"
		+ "SELECT _FUNC_(null,'string or date or any other datatype possible too') FROM src;\n")
public class MyNVL extends GenericUDF {

	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
	private ObjectInspector[] argumentOIs;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		argumentOIs = arguments;
		
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException("Function "
					+ this.getClass().getName() + " requires two arguments.");
		}

		if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				|| arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE)
				throw new UDFArgumentTypeException(0,
				"Function "
					+ this.getClass().getName() + " accepts only primitive data types.");

		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(
				true);
		
        if (!(returnOIResolver.update(arguments[0]) 
				&& returnOIResolver.update(arguments[1]))) {
			throw new UDFArgumentException("The arguments of the function "
					+ this.getClass().getName()
					+ " must have the same data type,"
					+ " but they are different: " + arguments[0].getTypeName()
					+ " and " + arguments[1].getTypeName() + ".");
		}
		
		
		return returnOIResolver.get();
		//return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		/*
		 * code from Book "programming Hive" does not work. java Exception with
		 * kryo ... Object retVal =
		 * returnOIResolver.convertIfNecessary(arguments[0].get(),
		 * argumentOIs[0]); if (retVal == null) { retVal =
		 * returnOIResolver.convertIfNecessary(arguments[1].get(),
		 * argumentOIs[1]); } return retVal; .. using
		 * https://github.com/nexr/hive
		 * -udf/blob/master/src/main/java/com/nexr/platform
		 * /hive/udf/GenericUDFNVL.java .... but the same :(
		 */
		
		Object returnValue = null;
		
		if (arguments[0].get() == null) {
			// fieldValue is null, return defaultValue
			returnValue = returnOIResolver.convertIfNecessary(
					arguments[1].get(), argumentOIs[1]);
		} else {
			returnValue = returnOIResolver.convertIfNecessary(
					arguments[0].get(), argumentOIs[0]);
		}
		return returnValue;
		//return new Integer(42);
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("if ");
		sb.append(children[0]);
		sb.append(" is null returns ");
		sb.append(children[1]);
		return sb.toString();
	}
}
