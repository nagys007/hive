package com.acme.hive.hiveinputformat;

/*

add jar hiveinputformat-0.0.1-SNAPSHOT.jar;

drop table if exists dual2;

create table dual2 ( dummy string ) stored as
inputformat 'de.metafinanz.sny.hiveinputformat.DualInputFormat'
outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

select * from dual2; -- returns no rows!!! because no mapred and no file

select dummy from dual2; -- returns 1 row with X

select count(1) from dual2; -- returns 1

 * */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

public class DualInputFormat implements InputFormat {

	public InputSplit[] getSplits(JobConf jc, int i) throws IOException {
		DualInputSplit[] splits = new DualInputSplit[1];
		splits[0] = new DualInputSplit();
		return splits;
	}
	
	public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf jc, Reporter r) throws IOException {
		return new DualRecordReader(jc, split);
	}
}
