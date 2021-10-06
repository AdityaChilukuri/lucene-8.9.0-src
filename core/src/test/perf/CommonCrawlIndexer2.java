package perf;

import java.io.*;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.util.fst.FST;

import java.lang.reflect.Executable;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// rm -rf /l/scratch/indices/geonames; pushd core; ant jar; popd; javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexGeoNames2.java; java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames2 /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames

// java CommonCrawlIndexer /home/adityac/sdb-mnt/adityac/commoncrawl/wets/

public class CommonCrawlIndexer2 {
    static final int NUM_PROCESSORS = 48;
    static volatile long[] docsCount = new long[1];
    static final AtomicLong datasetSize = new AtomicLong(0);
    static final AtomicLong documentCount = new AtomicLong(0);
    public static class MyDataInputStream extends DataInputStream {
        /**
         * Creates a DataInputStream that uses the specified
         * underlying InputStream.
         *
         * @param in the specified input stream
         */
        public MyDataInputStream(InputStream in) {
            super(in);
        }

        // Consumes input stream until "\r\n" is reached and returns the line.
        // Similar to the implementation in DataInputStream.readline() but is correct
        // for the purposes of parsing ISO-8859-1
        public String myReadLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            byte[] b = new byte[1];
            int state = 0;
            sb.append((char) this.readByte());
            while (true) {
                int len = this.read(b);
                if (len != 1)
                    throw new IllegalStateException();
                if (sb.charAt(sb.length() - 1) != '\r' || b[0] != (byte) '\n')
                    sb.append((char) b[0]);
                else
                    break;
            }
            return sb.substring(0, sb.length() - 1); // don't include the /r character
        }
    }

    public static void main(String args[]) throws Exception {
        String dataSetPath = args[0];
        String indexPath = args[1];

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        
        System.out.flush();
        final Codec codec = new Lucene87Codec();
        // This might need to be modified
        IndexWriter iw = new IndexWriter(dir,
                new IndexWriterConfig(new StandardAnalyzer())
                        //.setRAMBufferSizeMB(1)
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                        .setMergePolicy(new LogDocMergePolicy())
                        .setInfoStream(new PrintStreamInfoStream(System.out))
                        // .setRAMBufferSizeMB(256.0)
                        .setCodec(codec)
        );

        final Path docDir = Paths.get(dataSetPath);

        
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        File docsDirFile = new File(docDir.toString());
        final String[] watFileList = docsDirFile.list((current, name) -> new File(current, name).isFile());

        System.out.println(Arrays.toString(watFileList));

        long numDirs = watFileList.length;
        class IndexerThread extends Thread {
            final int startIndex, endIndex;
            volatile boolean v = false;
            IndexerThread(int start, int end) {
                this.startIndex = start;
                this.endIndex = end;
            }
            @Override
            public void run() {
                for (int i = startIndex; i < endIndex; i++) {
                    System.out.println("testing-print " + i);
                    try {
                        long size = indexDocs(iw, Paths.get(dataSetPath +"/"+ watFileList[i]));
                        CommonCrawlIndexer2.datasetSize.addAndGet(size);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                }
            }
        }
        ExecutorService exec = Executors.newFixedThreadPool(96);
        for (int i = 0; i < NUM_PROCESSORS; i++) {
            Thread t = new IndexerThread((int)(((double)(i*numDirs)) / NUM_PROCESSORS), (int)(((double)((i+1)*numDirs)) / NUM_PROCESSORS));
            exec.execute(t);
        }
        exec.shutdown();
        exec.awaitTermination(1000, TimeUnit.HOURS);

        System.out.println(CommonCrawlIndexer2.datasetSize.get() / 1000 + "kb is the total English raw text data size used");

        iw.close();
        dir.close();
    }

    static long indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            long[] docsSize = new long[1];
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        docsSize[0] += indexDoc(writer, file);
                        docsCount[0] += 1;
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            return docsSize[0];
        } else {
            return indexDoc(writer, path);
        }
    }

    /** Indexes a single document */
    static long indexDoc(IndexWriter writer, Path file) throws IOException {
//        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
//
////            if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
////                // New index, so we just add the document (no old document can be there):
////                System.out.println("adding " + file);
////                writer.addDocument(doc);
////            } else {
////                // Existing index (an old copy of this document may have been indexed) so
////                // we use updateDocument instead to replace the old one matching the exact
////                // path, if present:
////                System.out.println("updating " + file);
////                writer.updateDocument(new Term("path", file.toString()), doc);
////            }
//        }
        MyDataInputStream mdis = new MyDataInputStream(new FileInputStream(file.toAbsolutePath().toString()));
        String currentLine;
        long totalLength = 0;
        while (true) {
            try {
                while ((currentLine = mdis.myReadLine()).equals(""));

//                String request = mdis.myReadLine();
                Map<String, List<String>> headers = new HashMap<>();

                while (!(currentLine = mdis.myReadLine()).equals("")) {
                    int splitIndex = currentLine.indexOf(':');
                    headers.computeIfAbsent(currentLine.substring(0, splitIndex),
                            _a -> new LinkedList<>()).add(currentLine.substring(splitIndex + 1).trim());
                }
                if (!headers.containsKey("Content-Type")) {
                    throw new IllegalArgumentException("No Content-Type");
                }
                if (!headers.containsKey("Content-Length")) {
                    throw new IllegalArgumentException("No Content-Type");
                }
                var body = mdis.readNBytes(Integer.parseInt(headers.get("Content-Length").get(0)));
                // make a new, empty document


                if (headers.get("WARC-Identified-Content-Language") != null) {
                    if (headers.get("WARC-Identified-Content-Language").get(0).substring(0,3).equals("eng")) {
                        if (headers.get("Content-Type").get(0).contains("text")) {
                            totalLength += Integer.parseInt(headers.get("Content-Length").get(0));
                            Document doc = new Document();
                            // Add the path of the file as a field named "path".  Use a
                            // field that is indexed (i.e. searchable), but don't tokenize
                            // the field into separate words and don't index term frequency
                            // or positional information:
                            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                            doc.add(pathField);

                            for (var key : headers.keySet()) {
                                Field watHeader = new StringField(key, headers.get(key).get(0), Field.Store.YES);
                            }
                            // Add the contents of the file to a field named "contents".  Specify a Reader,
                            // so that the text of the file is tokenized and indexed, but not stored.
                            // Note that FileReader expects the file to be in UTF-8 encoding.
                            // If that's not the case searching for special characters will fail.
                            doc.add(new TextField("content",
                                    new InputStreamReader(new ByteArrayInputStream(body))));
                            writer.addDocument(doc);
                            long docsCount;
                            if ((docsCount = CommonCrawlIndexer2.documentCount.incrementAndGet()) % 100000 == 0) {
                                System.out.println("Docs count: " + docsCount);
                            }
                        }
                    }
                }

            } catch (EOFException ignored) {
                System.out.println("Made it to the end");
                break;
            }
        }
        System.out.println("Total english content length: " + totalLength / 1024 + "kb");
        return totalLength;
    }
}
