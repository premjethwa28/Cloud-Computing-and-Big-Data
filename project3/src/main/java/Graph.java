import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable {
    public short tag; // 0 for a graph vertex, 1 for a group number
    public long group; // the group where this vertex belongs to
    public long VID; // the vertex ID
    public long[] adjacent; // the vertex neighbors

    Vertex(short tag, long group, long VID, long[] adjacent) {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    Vertex() {
    }

    Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
    }

    public void print() {
        System.out.println(tag + "," + group + "," + VID + "," + adjacent);
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(this.tag);
        out.writeLong(this.group);
        out.writeLong(this.VID);
        if(this.adjacent != null) {
            out.writeInt(this.adjacent.length);
            for (int i = 0; i < this.adjacent.length; i++) {
                out.writeLong(this.adjacent[i]);
            }
        } else {
            out.writeInt(0);
        }
        
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        int x = in.readInt();
        long[] xx = new long[x];
        if(x != 0) {
            for (int i = 0; i < x; i++) {
                xx[i] = in.readLong();
            }
        }
        this.adjacent = xx;
    }
}

// *********------Mapper 1 Starts------*********
// map ( key, line ) =
// parse the line to get the vertex VID and the adjacent vector
// emit( VID, new Vertex(0,VID,VID,adjacent) )
public class Graph {
    public static class PAJ_Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Vector<Long> v = new Vector<Long>();
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = 0;
            Integer zero = 0;
            long vid = 0;
            Integer ivid = 0;
            while (s.hasNext()) {
                ivid = s.nextInt();
                if (x == 0) {
                    vid = ivid.longValue();
                    x = 1;
                } else {
                    long avid = ivid.longValue();
                    v.add(avid);
                }
            }
            long[] adjacent = new long[v.size()];
            for (int i = 0; i < v.size(); i++) {
                adjacent[i] = v.get(i);
            }
            // System.out.println();
            Vertex v1 = new Vertex(zero.shortValue(), vid, vid, adjacent);
            context.write(new LongWritable(vid), v1);
            s.close();
        }
    }
    // *********------Mapper 1 Ends------*********

    // *********------Mapper 2 Starts------*********
    // map ( key, vertex ) =
    // emit( vertex.VID, vertex ) // pass the graph topology
    // for n in vertex.adjacent:
    // emit( n, new Vertex(1,vertex.group) ) // send the group # to the adjacent
    // vertices
    public static class PAJ_Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        public void map(LongWritable key, Vertex v, Context context)
                throws IOException, InterruptedException {
            // System.out.println("debug-size"+v.adjacent.size());
            context.write(new LongWritable(v.VID), v);
            int size = v.adjacent.length;
            for (int i = 0; i < size; i++) {
                // System.out.println("debug adj:"+v.adjacent.get(i));
                // System.out.println("debug gourp:"+v.group);
                short s = 1;
                context.write(new LongWritable(v.adjacent[i]), new Vertex(s, v.group));
            }
        }
    }

    public static long min(long a, long b) {
        if (a < b) {
            return a;
        } else {
            return b;
        }
    }
    // *********------Mapper 2 Ends------*********

    // *********------Reducer 2 Starts------*********
    // reduce ( vid, values ) =
    // m = Long.MAX_VALUE;
    // for v in values:
    // if v.tag == 0
    // then adj = v.adjacent.clone() // found the vertex with vid
    // m = min(m,v.group) // regardless of v.tag
    // emit( m, new Vertex(0,m,vid,adj) ) // new group #
    public static class PAJ_Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        public void reduce(LongWritable vid, Iterable<Vertex> values, Context context)
                throws IOException, InterruptedException {
            long m = Long.MAX_VALUE;
            Vector<Long> adj = new Vector<Long>();
            for (Vertex v : values) {
                if (v.tag == 0) {
                    // adj = v.adjacent.clone();
                    for (int i = 0; i < v.adjacent.length; i++) {
                        adj.add(v.adjacent[i]);
                    }
                }
                m = min(m, v.group);
            }
            short s = 0;
            long[] newAdj = new long[adj.size()];
            for (int i = 0; i < newAdj.length; i++) {
                newAdj[i] = adj.get(i);
            }
            context.write(new LongWritable(m), new Vertex(s, m, vid.get(), newAdj));
        }
    }
    // *********------Reducer 2 Ends------*********

    // *********------Final Mapper Starts------*********
    // map ( group, value ) =
    // emit(group,1)
    public static class PAJ_Mapper3 extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
        public void map(LongWritable key, Vertex v, Context context)
                throws IOException, InterruptedException {
            context.write(key, new LongWritable(1));
        }
    }
    // *********------Final Mapper Ends------*********

    // *********------Final Reducer Starts------*********
    // reduce ( group, values ) =
    // m = 0
    // for v in values
    // m = m+v
    // emit(group,m)
    public static class PAJ_Reducer3 extends
            Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long m = 0;
            for (LongWritable v : values) {
                m = m + v.get();
            }
            context.write(key, new LongWritable(m));
        }
    }
    // *********------Final Reducer Ends------*********

    public static void main(String[] args) throws Exception {
        Job paj_job1 = Job.getInstance();
        paj_job1.setJobName("PAJ_Job1");
        paj_job1.setJarByClass(Graph.class);
        paj_job1.setOutputKeyClass(LongWritable.class);
        paj_job1.setOutputValueClass(Vertex.class);
        paj_job1.setMapOutputKeyClass(LongWritable.class);
        paj_job1.setMapOutputValueClass(Vertex.class);
        paj_job1.setMapperClass(PAJ_Mapper1.class);
        paj_job1.setInputFormatClass(TextInputFormat.class);
        paj_job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(paj_job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(paj_job1, new Path(args[1] + "/f0"));
        paj_job1.waitForCompletion(true);
        for (short i = 0; i < 5; i++) {
            Job paj_job2 = Job.getInstance();
            paj_job2.setJobName("PAJ_Job2");
            paj_job2.setJarByClass(Graph.class);
            paj_job2.setOutputKeyClass(LongWritable.class);
            paj_job2.setOutputValueClass(Vertex.class);
            paj_job2.setMapOutputKeyClass(LongWritable.class);
            paj_job2.setMapOutputValueClass(Vertex.class);
            paj_job2.setMapperClass(PAJ_Mapper2.class);
            paj_job2.setReducerClass(PAJ_Reducer2.class);
            paj_job2.setInputFormatClass(SequenceFileInputFormat.class);
            paj_job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(paj_job2, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(paj_job2, new Path(args[1] + "/f" + (i + 1)));
            paj_job2.waitForCompletion(true);
        }
        Job paj_job3 = Job.getInstance();
        paj_job3.setJobName("PAJ_Job3");
        paj_job3.setJarByClass(Graph.class);
        paj_job3.setOutputKeyClass(LongWritable.class);
        paj_job3.setOutputValueClass(Vertex.class);
        paj_job3.setMapOutputKeyClass(LongWritable.class);
        paj_job3.setMapOutputValueClass(LongWritable.class);
        paj_job3.setMapperClass(PAJ_Mapper3.class);
        paj_job3.setReducerClass(PAJ_Reducer3.class);
        paj_job3.setInputFormatClass(SequenceFileInputFormat.class);
        paj_job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(paj_job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(paj_job3, new Path(args[2]));
        paj_job3.waitForCompletion(true);
    }
}
