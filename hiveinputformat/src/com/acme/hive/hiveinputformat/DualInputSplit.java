package com.acme.hive.hiveinputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class DualInputSplit  implements InputSplit {

	@Override
	public void readFields(DataInput arg0) throws IOException {
		//dummy;		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		//dummy;		
	}

	@Override
	public long getLength() throws IOException {
		return 1;
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[] { "dummy" };
	}

}
