package iox.rdf2d4m;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
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

import iox.accumulo.AccumuloAccess;

public class RDF2D4MDriver implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(RDF2D4MDriver.class);

	public static final String ACCUMULO_INSTANCE = "accumuloInstance";

	public static final String ZOOKEEPER_URI = "zookeeperURI";

	public static final String TABLE_NAME = "tableName";

	public static final String OVERWRITE = "overwrite";

	public static final String ACCUMULO_CREDS_FILE = "accumuloCredsFile";

	public static final String ACCUMULO_CREDENTIALS = "accumuloCredentials";

	public static enum COUNTER {
		RECORDS
	}

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

	@Option(name = "-r", aliases = "--recurse", required = false, usage = "Set to reacuse throught directories. default = false;")
	private boolean recurse;

	private static RDFFormat rdfFormat = RDFFormat.NTRIPLES;

	String configFile;

	public RDF2D4MDriver(String[] args) throws CmdLineException {
		super();
		CmdLineParser CLI = new CmdLineParser(this);
		try {
			CLI.parseArgument(args);
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			throw e;
		}
		log.info(this.getClass().getName() + "==>");
	}

	@Override
	public void run() {
		log.debug("0==>");
		Configuration conf = new Configuration();
		conf.addResource(new Path(configFilePath + "/core-site.xml"));
		conf.addResource(new Path(configFilePath + "/hdfs-site.xml"));
		conf.set(ACCUMULO_CREDS_FILE, "file:///home/haz/accumulo-creds.yml");
		conf.set("xmlinput.start", "");
		conf.set("xmlinput.end", "");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		conf.set(ACCUMULO_INSTANCE, accumuloInstance);
		conf.set(TABLE_NAME, tableName);
		conf.setBoolean(OVERWRITE, overwrite);
		conf.set(ZOOKEEPER_URI, zookeeperURI);

		try {
			URI credsFile = new URI(conf.get(ACCUMULO_CREDS_FILE));
			byte[] encoded = Files.readAllBytes(Paths.get(credsFile));
			String credentials = new String(encoded, "UTF-8");
			conf.set(ACCUMULO_CREDENTIALS, credentials);
			log.debug("credentials=" + credentials);
			Job job = Job.getInstance(conf);
			job.setJarByClass(RDF2D4MDriver.class);
			FileSystem fs = FileSystem.get(new java.net.URI("hdfs://haz00:9000"), conf);
			AccumuloAccess acc = new AccumuloAccess(zookeeperURI, accumuloInstance, credentials);
			Connector conn = acc.getConnection();
			if (overwrite) {
				log.info("Overwrite was set.  Delete/create tables.");
				if (conn.tableOperations().exists(tableName)) {
					log.info("  Deleting " + tableName);
					conn.tableOperations().delete(tableName);
				}
				if (conn.tableOperations().exists(tableName + "T")) {
					log.info("  Deleting " + tableName + "T");
					conn.tableOperations().delete(tableName + "T");
				}
				if (conn.tableOperations().exists(tableName + "Deg")) {
					log.info("  Deleting " + tableName + "Deg");
					conn.tableOperations().delete(tableName + "Deg");
				}
				if (conn.tableOperations().exists(tableName + "DegT")) {
					log.info("  Deleting " + tableName + "DegT");
					conn.tableOperations().delete(tableName + "DegT");
				}
			}
			if (!conn.tableOperations().exists(tableName)) {
				log.info("  Creating " + tableName);
				conn.tableOperations().create(tableName);
			}
			if (!conn.tableOperations().exists(tableName + "T")) {
				log.info("  Creating " + tableName + "T");
				conn.tableOperations().create(tableName + "T");
			}
			if (!conn.tableOperations().exists(tableName + "Deg")) {
				log.info("  Creating " + tableName + "Deg");
				conn.tableOperations().create(tableName + "Deg");
			}
			if (!conn.tableOperations().exists(tableName + "DegT")) {
				log.info("  Creating " + tableName + "DegT");
				conn.tableOperations().create(tableName + "DegT");
			}

			Path pathRoot = new Path(fs.getUri());

			Path pathInput = new Path(pathRoot, input);
			log.info("pathInput=" + pathInput.toString());

			Path pathOutput = new Path(pathRoot, "/null");
			log.info("pathOutput=" + pathOutput.toString());

			job.setMapperClass(RDF2D4MMapper.class);
			job.setNumReduceTasks(0);

			FileInputFormat.setInputPaths(job, pathInput);
			FileInputFormat.setInputDirRecursive(job, recurse);
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

	static Map<String, String> merge(String[] args, Map<String, String> cfg) {
		for (int i = 0; i < args.length; i += 2) {
			cfg.put(args[i].replace("-", ""), args[i + 1]);
		}
		return cfg;
	}

	public static void main(String[] args) {
		try {
			RDF2D4MDriver app = new RDF2D4MDriver(args);
			log.info("Start==>");
			app.run();
			log.info("<==Finish");
		} catch (CmdLineException e) {
			log.error("Soaping is wrong.", e);
		}
	}
}
