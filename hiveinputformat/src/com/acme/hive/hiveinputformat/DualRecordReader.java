package com.acme.hive.hiveinputformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputSplit;

public class DualRecordReader implements RecordReader<Text, Text> {

	boolean hasNext = true;

	public DualRecordReader(JobConf jc, InputSplit s) {
		// dummy
	}

	public DualRecordReader() {
		// dummy
	}

	@Override
	public void close() throws IOException {
		// dummy
	}

	public Text createKey() {
		return new Text("k");
	}

	public Text createValue() {
		return new Text("X");
	}

	@Override
	public long getPos() throws IOException {
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		if (hasNext)
			return 0.0f;
		else
			return 1.0f;
	}

	@Override
	public boolean next(Text arg0, Text arg1) throws IOException {
		if (hasNext) {
			hasNext = false;
			return true;
		} else
			return false;
	}

}
