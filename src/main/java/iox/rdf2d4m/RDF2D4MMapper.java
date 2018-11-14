package iox.rdf2d4m;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.mit.ll.graphulo.util.D4MTableWriter;
import edu.mit.ll.graphulo.util.D4MTableWriter.D4MTableConfig;
import iox.accumulo.AccumuloAccess;

public class RDF2D4MMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static final Logger log = LoggerFactory.getLogger(RDF2D4MMapper.class);

	private static RDFFormat rdfFormat = RDFFormat.NTRIPLES;

	D4MTableWriter d4mTW;

	enum RDF {
		SUBJECT, PREDICATE, OBJECT
	};
	
	boolean first;
	String lastValue;

	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context ctx)
			throws IOException, InterruptedException {
		log.trace("setup==>0");
		super.setup(ctx);
		log.trace("setup==>1");

		log.trace("setup==>3");
		Configuration cfg = ctx.getConfiguration();
		String zookeeperURI = cfg.get(RDF2D4MDriver.ZOOKEEPER_URI);
		String accumuloInstance = cfg.get(RDF2D4MDriver.ACCUMULO_INSTANCE);
		String tableName = cfg.get(RDF2D4MDriver.TABLE_NAME);
		String credentials = cfg.get(RDF2D4MDriver.ACCUMULO_CREDENTIALS);
		AccumuloAccess db = new AccumuloAccess(zookeeperURI, accumuloInstance, credentials);
		D4MTableConfig tconf = new D4MTableConfig();
		tconf.baseName = tableName;
		tconf.connector = db.getConnection();
		tconf.useTable = true;
		tconf.useTableT = true;
		tconf.useTableDeg = true;
		tconf.useTableDegT = true;
		d4mTW = new D4MTableWriter(tconf);
		D4MTableWriter.createTableSoft(tableName, db.getConnection(), false);
		first = true;
		log.debug("<==setup" + " zoo=" + zookeeperURI);
	}

	@Override
	protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		ctx.getCounter(RDF2D4MDriver.COUNTER.RECORDS).increment(1);
		log.trace("map==>0");
		lastValue = value.toString();
		if (value.getLength() == 0) {
			return;
		}
		if (first) {
			log.info("first v=" + value.toString() + " count=" + ctx.getCounter(RDF2D4MDriver.COUNTER.RECORDS).getValue());
			first = false;
		}
		String s = value.toString().replaceAll("[<>\"]", "");
		if (s.trim().length() == 0) {
			return;
		}
		log.trace("map==>1");

		String[] spo = s.split(" ");
		try {
			d4mTW.ingestRow(new Text(spo[RDF.SUBJECT.ordinal()]), new Text(spo[RDF.PREDICATE.ordinal()]),
					new Value(spo[RDF.OBJECT.ordinal()]));
		} catch (ArrayIndexOutOfBoundsException e) {
			log.error("v=" + value.toString() + " count=" + ctx.getCounter(RDF2D4MDriver.COUNTER.RECORDS).getValue());
			log.error("s=" + s + " count=" + ctx.getCounter(RDF2D4MDriver.COUNTER.RECORDS).getValue());
			log.error("spo=" + Arrays.toString(spo));
		} catch (Exception e) {
			log.error("", e);
		}
		if (ctx.getCounter(RDF2D4MDriver.COUNTER.RECORDS).getValue() % 5000 == 0) {
			log.debug("inserted=" + ctx.getCounter(RDF2D4MDriver.COUNTER.RECORDS).getValue());
			d4mTW.flushBuffers();
		}
		log.trace("<==map");
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context ctx)
			throws IOException, InterruptedException {
		super.cleanup(ctx);
		log.info("cleanup v=" + lastValue + " count=" + ctx.getCounter(RDF2D4MDriver.COUNTER.RECORDS).getValue());
		d4mTW.flushBuffers();
		d4mTW.close();
	}

}
