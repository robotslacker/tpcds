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

import org.apache.hadoop.fs.FileSystem;

import java.util.Optional;

public class Session
{
    public static final int DEFAULT_SCALE = 1;
    public static final String DEFAULT_DIRECTORY = ".";
    public static final String DEFAULT_SUFFIX = ".dat";
    public static final String DEFAULT_TABLE = null;
    public static final String DEFAULT_NULL_STRING = "";
    public static final char DEFAULT_SEPARATOR = '|';
    public static final boolean DEFAULT_DO_NOT_TERMINATE = false;
    public static final boolean DEFAULT_NO_SEXISM = false;
    public static final int DEFAULT_PARALLELISM = 1;
    public static final boolean DEFAULT_OVERWRITE = false;

    private final Scaling scaling;
    private final String targetDirectory;
    private final String suffix;
    private final Optional<Table> table;
    private final String nullString;
    private final char separator;
    private final boolean doNotTerminate;
    private final boolean noSexism;
    private final int parallelism;
    private final int chunkNumber;
    private final boolean overwrite;

    public Session(double scale, String targetDirectory, String suffix, Optional<Table> table, String nullString, char separator, boolean doNotTerminate, boolean noSexism, int parallelism, boolean overwrite)
    {
        this(scale, targetDirectory, suffix, table, nullString, separator, doNotTerminate, noSexism, parallelism, 1, overwrite);
    }

    public Session(double scale, String targetDirectory, String suffix, Optional<Table> table, String nullString, char separator, boolean doNotTerminate, boolean noSexism, int parallelism, int chunkNumber, boolean overwrite)
    {
        this.scaling = new Scaling(scale);
        this.targetDirectory = targetDirectory;
        this.suffix = suffix;
        this.table = table;
        this.nullString = nullString;
        this.separator = separator;
        this.doNotTerminate = doNotTerminate;
        this.noSexism = noSexism;
        this.parallelism = parallelism;
        this.chunkNumber = chunkNumber;
        this.overwrite = overwrite;
    }

    public Session withTable(Table table)
    {
        return new Session(
                this.scaling.getScale(),
                this.targetDirectory,
                this.suffix,
                Optional.of(table),
                this.nullString,
                this.separator,
                this.doNotTerminate,
                this.noSexism,
                this.parallelism,
                this.chunkNumber,
                this.overwrite);
    }

    public Session withScale(double scale)
    {
        return new Session(
                scale,
                this.targetDirectory,
                this.suffix,
                this.table,
                this.nullString,
                this.separator,
                this.doNotTerminate,
                this.noSexism,
                this.parallelism,
                this.chunkNumber,
                this.overwrite);
    }

    public Session withParallelism(int parallelism)
    {
        return new Session(
                this.scaling.getScale(),
                this.targetDirectory,
                this.suffix,
                this.table,
                this.nullString,
                this.separator,
                this.doNotTerminate,
                this.noSexism,
                parallelism,
                this.chunkNumber,
                this.overwrite);
    }

    public Session withChunkNumber(int chunkNumber)
    {
        return new Session(
                this.scaling.getScale(),
                this.targetDirectory,
                this.suffix,
                this.table,
                this.nullString,
                this.separator,
                this.doNotTerminate,
                this.noSexism,
                this.parallelism,
                chunkNumber,
                this.overwrite);
    }

    private static Optional<Table> toTableOptional(String table)
    {
        if (table == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(Table.valueOf(table.toUpperCase()));
        }
        catch (IllegalArgumentException e) {
            throw new InvalidOptionException("table", table);
        }
    }

    public static Session getDefaultSession()
    {
        return new Session(
                DEFAULT_SCALE,
                DEFAULT_DIRECTORY,
                DEFAULT_SUFFIX,
                toTableOptional(DEFAULT_TABLE),
                DEFAULT_NULL_STRING,
                DEFAULT_SEPARATOR,
                DEFAULT_DO_NOT_TERMINATE,
                DEFAULT_NO_SEXISM,
                DEFAULT_PARALLELISM,
                DEFAULT_OVERWRITE);
    }

    public Session withNoSexism(boolean noSexism)
    {
        return new Session(
                this.scaling.getScale(),
                this.targetDirectory,
                this.suffix,
                this.table,
                this.nullString,
                this.separator,
                this.doNotTerminate,
                noSexism,
                this.parallelism,
                this.chunkNumber,
                this.overwrite);
    }

    public Scaling getScaling()
    {
        return scaling;
    }

    public String getTargetDirectory()
    {
        return targetDirectory;
    }

    public String getSuffix()
    {
        return suffix;
    }

    public boolean generateOnlyOneTable()
    {
        return table.isPresent();
    }

    public Table getOnlyTableToGenerate()
    {
        if (!table.isPresent()) {
            throw new TpcdsException("table not present");
        }
        return table.get();
    }

    public String getNullString()
    {
        return nullString;
    }

    public char getSeparator()
    {
        return separator;
    }

    public boolean terminateRowsWithSeparator()
    {
        return !doNotTerminate;
    }

    public boolean isSexist()
    {
        return !noSexism;
    }

    public int getParallelism()
    {
        return parallelism;
    }

    public int getChunkNumber()
    {
        return chunkNumber;
    }

    public boolean shouldOverwrite()
    {
        return overwrite;
    }

    public String getCommandLineArguments()
    {
        StringBuilder output = new StringBuilder();
        if (scaling.getScale() != DEFAULT_SCALE) {
            output.append("--scale ").append(scaling.getScale()).append(" ");
        }
        if (!targetDirectory.equals(DEFAULT_DIRECTORY)) {
            output.append("--directory ").append(targetDirectory).append(" ");
        }
        if (!suffix.equals(DEFAULT_SUFFIX)) {
            output.append("--suffix ").append(suffix).append(" ");
        }
        if (table.isPresent()) {
            output.append("--table ").append(table.get().getName()).append(" ");
        }
        if (!nullString.equals(DEFAULT_NULL_STRING)) {
            output.append("--null ").append(nullString).append(" ");
        }
        if (separator != DEFAULT_SEPARATOR) {
            output.append("--separator ").append(separator).append(" ");
        }
        if (doNotTerminate != DEFAULT_DO_NOT_TERMINATE) {
            output.append("--do-not-terminate ");
        }
        if (noSexism != DEFAULT_NO_SEXISM) {
            output.append("--no-sexism ");
        }
        if (parallelism != DEFAULT_PARALLELISM) {
            output.append("--parallelism ").append(parallelism).append(" ");
        }
        if (overwrite != DEFAULT_OVERWRITE) {
            output.append("--overwrite ");
        }

        // remove trailing space
        if (output.length() > 0) {
            output.deleteCharAt(output.length() - 1);
        }

        return output.toString();
    }
}
