package iox.rdf2d4m;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDF2D4MDriver implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(RDF2D4MDriver.class);
	
	public static final String CONFIG_FILE = "configFile";

	@Option(name = "-i", aliases = "--input", required = false, usage = "")
	private String input;
	
	@Option(name = "-o", aliases = "--output", required = true, usage = "hdfs Path to output. dir only")
	private String output;

	@Option(name = "-ow", aliases = "--write", required = false, usage = "Overwrite output")
	private boolean overwrite;
	
	@Option(name = "-c", aliases = "--config", required = false, usage = "Path to config file.")
	private String configFileName;
	

	private static RDFFormat rdfFormat = RDFFormat.NTRIPLES;
	private Authorizations auths = new Authorizations("scap_auths");
	static Properties props = new Properties();
	static Connector conn;
	
	public RDF2D4MDriver(String[] args) throws CmdLineException, IllegalArgumentException {
		super();
		CmdLineParser CLI = new CmdLineParser(this);
		try {
			CLI.parseArgument(args);
		} catch (CmdLineException | IllegalArgumentException e) {
			CLI.printUsage(System.out);
		}
		log.info(this.getClass().getName() + "==>");
	}

	@Override
	public void run() {
		Configuration conf = new Configuration();
		final String HADOOP_CONF = "/usr/local/hadoop/etc/hadoop";
		conf.addResource(new Path(HADOOP_CONF + "/core-site.xml"));
		conf.addResource(new Path(HADOOP_CONF + "/hdfs-site.xml"));
		conf.set("xmlinput.start", "");
		conf.set("xmlinput.end", "");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		
		try {
			URI uriConfig = new URI(configFileName);
			String configFile = new String(Files.readAllBytes(Paths.get(uriConfig)));
			conf.set(CONFIG_FILE, configFile);
			Job job = Job.getInstance(conf);
			job.setJarByClass(RDF2D4MDriver.class);
			FileSystem fs = FileSystem.get(new java.net.URI("hdfs://haz00:9000"),
					conf);
			FileStatus[] ffss = fs.listStatus(new Path("/libs/rdf2d4m/lib"));
			for (FileStatus fs1 : ffss) {
				job.addArchiveToClassPath(fs1.getPath());
			}
			Path pathRoot = new Path(fs.getUri());
			
			Path pathInput = new Path(pathRoot, input);
			log.info("pathInput=" + pathInput.toString());
			Path pathOutput = new Path(pathInput, "/" + output);
			log.info("pathOutput=" + pathOutput.toString());
			
			if (fs.exists(pathOutput)) {
				fs.delete(pathOutput, overwrite);
			}

			job.setMapperClass(RDF2D4MMapper.class);
			job.setNumReduceTasks(0);

			FileInputFormat.setInputPaths(job, pathInput);
			FileOutputFormat.setOutputPath(job, pathOutput);
			job.setInputFormatClass(TextInputFormat.class);

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

	public static void main(String[] args) {
		try {
			RDF2D4MDriver app = new RDF2D4MDriver(args);
			app.run();
		} catch (CmdLineException e) {
			log.error("", e.fillInStackTrace());
		}
	}
}
