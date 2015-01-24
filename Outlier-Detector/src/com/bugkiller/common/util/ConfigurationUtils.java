package com.bugkiller.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ConfigurationUtils {

	public static Configuration getConfiguration(){
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		return conf;
	}
	
}
