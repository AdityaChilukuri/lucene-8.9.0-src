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

import java.lang.reflect.Executable;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// rm -rf /l/scratch/indices/geonames; pushd core; ant jar; popd; javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexGeoNames2.java; java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames2 /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames

// java CommonCrawlIndexer /home/adityac/sdb-mnt/adityac/commoncrawl/wets/

public class CommonCrawlIndexer {
    static final int NUM_PROCESSORS = 96;
    static volatile long[] docsCount = new long[1];
    public static void main(String args[]) throws Exception {
        String dataSetPath = args[0];
        String indexPath = args[1];

        Directory dir = FSDirectory.open(Paths.get(indexPath));

        final Codec codec = new Lucene87Codec();
        // This might need to be modified
        IndexWriter iw = new IndexWriter(dir,
                new IndexWriterConfig(new StandardAnalyzer())
                        //.setRAMBufferSizeMB(1)
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                        .setMergePolicy(new LogDocMergePolicy())
                        .setInfoStream(new PrintStreamInfoStream(System.out))
                        .setRAMBufferSizeMB(256.0)
                        .setCodec(codec)
        );

        final Path docDir = Paths.get(dataSetPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        File docsDirFile = new File(docDir.toString());
        final String[] subDirectories = docsDirFile.list((current, name) -> new File(current, name).isDirectory());

        System.out.println(Arrays.toString(subDirectories));
        long numDirs = subDirectories.length;
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
                        indexDocs(iw, Paths.get(dataSetPath +"/"+ subDirectories[i]));   
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
        exec.awaitTermination(1000, TimeUnit.HOURS);
        exec.shutdown();
//        Arrays.asList(subDirectories).parallelStream().forEach(x);
//        Arrays.parallel(subDirectory -> {
//            Thread t = new Thread
//            try {
//                indexDocs(iw, Paths.get(dataSetPath +"/"+ subDirectory));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });

//        FileInputStream fs = new FileInputStream();
//        BufferedReader r = new BufferedReader(new InputStreamReader(fs, "UTF-8"));
//        String line = null;
//        Document doc = new Document();
//
//        for (int i = 0; i < fields.length; i++) {
//            fields[i] = new StringField("" + i, "", Field.Store.NO);
//            doc.add(fields[i]);
//        }
//        int docCount = 0;
//        long prev = System.currentTimeMillis();
//        while ((line = r.readLine()) != null) {
//            if ((++docCount % 10000) == 0) {
//                long curr = System.currentTimeMillis();
//                System.out.println("Indexed: " + docCount + " (" + (curr - prev) + ")");
//                prev = curr;
//            }
//            String parts[] = line.split("\t");
//            for (int i = 0; i < fields.length; i++) {
//                fields[i].setStringValue(parts[i]);
//            }
//            iw.addDocument(doc);
//        }
        iw.close();
        dir.close();
    }

    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        indexDoc(writer, file);
                        docsCount[0] += 1;
                        if ((docsCount[0] % 10000) == 0) {
                            System.out.println("Indexed " + docsCount[0] + " files");
                        }
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path);
        }
    }

    /** Indexes a single document */
    static void indexDoc(IndexWriter writer, Path file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            // make a new, empty document
            Document doc = new Document();

            // Add the path of the file as a field named "path".  Use a
            // field that is indexed (i.e. searchable), but don't tokenize
            // the field into separate words and don't index term frequency
            // or positional information:
            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);
            // Also store the URL
            String url = reader.readLine();
            if (url != null) {
                Field urlField = new StringField("url", url, Field.Store.YES);
                doc.add(urlField);

                // Add the contents of the file to a field named "contents".  Specify a Reader,
                // so that the text of the file is tokenized and indexed, but not stored.
                // Note that FileReader expects the file to be in UTF-8 encoding.
                // If that's not the case searching for special characters will fail.
                doc.add(new TextField("contents", reader));
                writer.addDocument(doc);
            }
//            if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
//                // New index, so we just add the document (no old document can be there):
//                System.out.println("adding " + file);
//                writer.addDocument(doc);
//            } else {
//                // Existing index (an old copy of this document may have been indexed) so
//                // we use updateDocument instead to replace the old one matching the exact
//                // path, if present:
//                System.out.println("updating " + file);
//                writer.updateDocument(new Term("path", file.toString()), doc);
//            }
        }
    }
}
