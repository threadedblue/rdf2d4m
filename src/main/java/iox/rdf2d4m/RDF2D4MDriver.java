package iox.rdf2d4m;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import iox.accumulo.AccumuloAccess;

public class RDF2D4MDriver implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(RDF2D4MDriver.class);

	public static final String CONFIG_FILE = "file://conf/rdf2d4m.yml";

	public static final String ACCUMULO_INSTANCE = "accumuloInstance";

	public static final String ZOOKEEPER_URI = "zookeeperURI";

	public static final String TABLE_NAME = "tableName";

	public static final String OVERWRITE = "overwrite";

	public static final String ACCUMULO_CREDS_FILE = "accumuloCredsFile";

	private static RDF2D4MConfig config;

	@Option(name = "-i", aliases = "--input", required = true, usage = "")
	private String input;

	@Option(name = "-ow", aliases = "--overwrite", required = false, usage = "Overwrite output")
	private boolean overwrite;

	@Option(name = "-c", aliases = "--config", required = false, usage = "Path to hadoop config directory.")
	private String configFilePath = "/usr/local/hadoop/etc/hadoop";

	@Option(name = "-fs", aliases = "--filesystem", required = true, usage = "URL to the hadoop file system as a string.")
	private String fileSystem;

	@Option(name = "-l", aliases = "--instance", required = false, usage = "Name of Accumulo instance.")
	private String accumuloInstance;

	@Option(name = "-zk", aliases = "--zookeeper", required = true, usage = "URL to zookeeper instance as a string.")
	private String zookeeperURI;

	@Option(name = "-t", aliases = "--tablename", required = true, usage = "Base name of the table set for D4M.")
	private String tableName;

	private static RDFFormat rdfFormat = RDFFormat.TURTLE;

	String configFile;

	public RDF2D4MDriver(String[] args) {
		CmdLineParser CLI = new CmdLineParser(this);
		try {
			log.debug("RDF2D4MDriver=0");
			String[] argArray = getConfig().getArgsAsArray();
			log.debug("RDF2D4MDriver=1");
			log.debug("RDF2D4MDriver=2" + Arrays.toString(argArray));
			CLI.parseArgument(argArray);
			log.debug("RDF2D4MDriver=3" + ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE));
		} catch (CmdLineException | IllegalArgumentException e) {
			CLI.printUsage(System.out);
		}
		log.info(this.getClass().getName() + "==>");
	}

	@Override
	public void run() {
		Configuration conf = new Configuration();
		conf.addResource(new Path(configFilePath + "/core-site.xml"));
		conf.addResource(new Path(configFilePath + "/hdfs-site.xml"));
		conf.set("xmlinput.start", "");
		conf.set("xmlinput.end", "");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		conf.set(ACCUMULO_INSTANCE, accumuloInstance);
		conf.set(ACCUMULO_CREDS_FILE, getConfig().getAccumuloCreds());
		conf.set(TABLE_NAME, tableName);
		conf.setBoolean(OVERWRITE, overwrite);
		conf.set(ZOOKEEPER_URI, zookeeperURI);

		try {
			log.debug("accumuloInstance=" + accumuloInstance);
			Job job = Job.getInstance(conf);
			job.setJarByClass(RDF2D4MDriver.class);
			FileSystem fs = FileSystem.get(new java.net.URI("hdfs://haz00:9000"), conf);
			FileStatus[] ffss = fs.listStatus(new Path("/libs/rdf2d4m/lib"));
			for (FileStatus fs1 : ffss) {
				job.addArchiveToClassPath(fs1.getPath());
			}
			if(overwrite) {
				URL credsFile = new URL(getConfig().getAccumuloCreds());
				AccumuloAccess acc = new AccumuloAccess(zookeeperURI, accumuloInstance, credsFile);
				Connector conn = acc.getConnection();
				if (conn.tableOperations().exists(tableName)) {
					conn.tableOperations().delete(tableName);
				}
				if (conn.tableOperations().exists(tableName + "T")) {
					conn.tableOperations().delete(tableName + "T");
				}
				if (conn.tableOperations().exists(tableName + "Deg")) {
					conn.tableOperations().delete(tableName + "Deg");
				}
				conn.tableOperations().create(tableName);
				conn.tableOperations().create(tableName + "T");
				conn.tableOperations().create(tableName + "Deg");
			}
			Path pathRoot = new Path(fs.getUri());

			Path pathInput = new Path(pathRoot, input);
			log.info("pathInput=" + pathInput.toString());
			
			Path pathOutput = new Path(pathRoot, "/null");
			log.info("pathOutput=" + pathOutput.toString());

			job.setMapperClass(RDF2D4MMapper.class);
			job.setNumReduceTasks(0);

			FileInputFormat.setInputPaths(job, pathInput);
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, pathOutput);
			job.setOutputFormatClass(NullOutputFormat.class);

			job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
			log.debug("waitForCompletion==>");
			job.waitForCompletion(true);
			log.debug("<==waitForCompletion");
		} catch (IOException e) {
			log.error("", e.fillInStackTrace());
		} catch (NullPointerException e) {
			log.error("", e.fillInStackTrace());
		} catch (Exception e) {
			log.error("", e.fillInStackTrace());
		}
	}

	static RDF2D4MConfig getConfig() {
		log.debug("getConfig=0");
		if (config == null) {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
					.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			log.debug("getConfig=1");
			try {
				URI uri = new URI(CONFIG_FILE);
				log.debug("getConfig=2");
				File file = new File("conf/rdf2d4m.yml");
				log.debug("getConfig=2.5");
				if (file.exists()) {
					log.debug("getConfig=3");
					log.debug("mapper=" + mapper);
					config = mapper.readValue(file, RDF2D4MConfig.class);
					log.debug(ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE));
				} else {
					String eol = System.getProperty("line.separator");
					StringReader reader = new StringReader("args:" + eol + "accumulo-creds: file:///~/accumulo-creds.yml");
					config = mapper.readValue(reader, RDF2D4MConfig.class);
					log.debug(ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE));
				}
			} catch (IOException | URISyntaxException e) {
				log.error("", e);
			}
		}
		return config;
	}

	static Map<String, String> merge(String[] args, Map<String, String> cfg) {
		for (int i = 0; i < args.length; i += 2) {
			cfg.put(args[i].replace("-", ""), args[i + 1]);
		}
		return cfg;
	}

	public static void main(String[] args) {
		Map<String, String> cfg = RDF2D4MDriver.getConfig().getArgs();
		merge(args, cfg);
		String[] args1 = RDF2D4MDriver.getConfig().getArgsAsArray();
		RDF2D4MDriver app = new RDF2D4MDriver(args1);
		app.run();
	}
}
