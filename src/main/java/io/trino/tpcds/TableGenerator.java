/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.tpcds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.tpcds.Results.constructResults;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableGenerator
{
    private final Session session;
    private boolean isSparkEnv;

    public TableGenerator(Session session, boolean isSparkEnv)
    {
        this.session = requireNonNull(session, "session is null");
        this.isSparkEnv = isSparkEnv;
    }

    public void generateTable(Table table)
    {
        // If this is a child table and not the only table being generated, it will be generated when its parent is generated, so move on.
        if (table.isChild() && !session.generateOnlyOneTable()) {
            return;
        }

        try
        {
            OutputStream parentWriter = addFileWriterForTable(table);
            OutputStream childWriter = table.hasChild() && !session.generateOnlyOneTable() ? addFileWriterForTable(table.getChild()) : null;
            Results results = constructResults(table, session);
            for (List<List<String>> parentAndChildRows : results) {
                if (parentAndChildRows.size() > 0) {
                    writeResults(parentWriter, parentAndChildRows.get(0));
                }
                if (parentAndChildRows.size() > 1) {
                    requireNonNull(childWriter, "childWriter is null, but a child row was produced");
                    writeResults(childWriter, parentAndChildRows.get(1));
                }
            }
        }
        catch (IOException e) {
            throw new TpcdsException(e.getMessage());
        }
    }

    private OutputStream addFileWriterForTable(Table table)
            throws IOException
    {
        String path = getPath(table);
        if (path.startsWith("hdfs://"))
        {
            Configuration configuration = new Configuration();
            if (!this.isSparkEnv)
            {
                // 本地HDFS环境，需要初始化core-site等信息，Spark环境不需要这些
                String m_HDFSConfPath = System.getenv("HADOOP_CONF_DIR");
                if (m_HDFSConfPath == null)
                {
                    throw new TpcdsException("Missed env HADOOP_CONF_DIR. Please make sure set it before launch this.");
                }
                String m_ConfFilePath = Paths.get(m_HDFSConfPath,"core-site.xml").toString();
                File m_ConfFile = new File(m_ConfFilePath);
                if (!m_ConfFile.exists())
                {
                    throw new TpcdsException("Wrong HADOOP_CONF_DIR. core-site.xml does not exist.");
                }
                configuration.addResource(new Path(m_ConfFilePath));
                configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            }
            FileSystem m_fs = FileSystem.get(configuration);
            if (m_fs == null)
            {
                throw new TpcdsException("init Hadoop file system failed.");
            }
            Path m_HdfsPath = new Path(path);
            Path m_HdfsParentPath = m_HdfsPath.getParent();
            if (!m_fs.exists(m_HdfsParentPath))
            {
                m_fs.mkdirs(m_HdfsParentPath);
            }
            else
            {
                if (! m_fs.isDirectory(m_HdfsParentPath))
                {
                    throw new TpcdsException("Target is not a directory. [" + m_HdfsParentPath.toString() + "]");
                }
            }
            boolean newFileCreated = m_fs.createNewFile(m_HdfsPath);
            if (!newFileCreated) {
                if (session.shouldOverwrite()) {
                    // truncate the file
                    m_fs.create(m_HdfsPath).close();
                } else {
                    throw new TpcdsException(format("File %s exists.  Remove it or run with the '--overwrite' option", path));
                }
            }
            return m_fs.create(m_HdfsPath).getWrappedStream();
        }
        else {
            File file = new File(path);
            File parent = file.getParentFile();
            if (parent != null)
            {
                Files.createDirectories(parent.toPath());
            }
            boolean newFileCreated = file.createNewFile();
            if (!newFileCreated) {
                if (session.shouldOverwrite()) {
                    // truncate the file
                    new FileOutputStream(path).close();
                } else {
                    throw new TpcdsException(format("File %s exists.  Remove it or run with the '--overwrite' option", path));
                }
            }
            return new FileOutputStream(path, true);
        }
    }

    private String getPath(Table table)
    {
        if (session.getParallelism() > 1) {
            return format("%s%s%s%s%s_%d_%d%s",
                    session.getTargetDirectory(),
                    File.separator,
                    table.getName(),
                    File.separator,
                    table.getName(),
                    session.getChunkNumber(),
                    session.getParallelism(),
                    session.getSuffix());
        }
        return format("%s%s%s%s%s%s",
                session.getTargetDirectory(),
                File.separator,
                table.getName(),
                File.separator,
                table.getName(),
                session.getSuffix());
    }

    private void writeResults(OutputStream writer, List<String> values)
            throws IOException
    {
        writer.write(formatRow(values, session).getBytes());
    }

    public static String formatRow(List<String> values, Session session)
    {
        // replace nulls with the string representation for null
        values = values.stream().map(value -> value != null ? value : session.getNullString()).collect(Collectors.toList());

        StringBuilder stringBuilder = new StringBuilder();
        char separator = session.getSeparator();
        stringBuilder.append(values.get(0));
        for (int i = 1; i < values.size(); i++) {
            stringBuilder.append(separator);
            stringBuilder.append(values.get(i));
        }
        if (session.terminateRowsWithSeparator()) {
            stringBuilder.append(separator);
        }
        stringBuilder.append('\n');
        return stringBuilder.toString();
    }
}
