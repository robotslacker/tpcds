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

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;

public class Driver
{
    public static void usage()
    {
        System.out.println("local:   java -jar tpcds-gen.jar OutPath Scale ThreadNumber");
        System.out.println("         OutPath:  root directory of location to create data in. default is local directory");
        System.out.println("         Scale  :  scaleFactor defines the size of the dataset to generate (in GB). default is 1");
        System.out.println("         Thread :  the parallel thread count for generate data. default is scale.");

    }
    public static void main(String[] args) {
        String m_OutPutPath = "";
        int m_Scale = 1;
        int m_Thread = -1;

        // 程序参数 path,Scale,[Thread]
        if (args.length >= 1) {
            m_OutPutPath = args[0].trim();
        }
        if (args.length >= 2) {
            if (StringUtils.isNumeric(args[1])) {
                m_Scale = Integer.parseInt(args[1]);
            } else {
                usage();
                System.exit(0);
            }
        }
        if (args.length >= 3) {
            if (StringUtils.isNumeric(args[2])) {
                m_Thread = Integer.parseInt(args[2]);
            } else {
                usage();
                System.exit(0);
            }
        }

        if (m_Thread == -1)
        {
            m_Thread = m_Scale;
            if (m_Thread > Runtime.getRuntime().availableProcessors()) {
                m_Thread = Runtime.getRuntime().availableProcessors();
            }
        }

        List<Table> tablesToGenerate;
        tablesToGenerate = Table.getBaseTables();

        for (int i = 1; i <= m_Thread; i++) {
            int         chunkNumber = i;
            int         finalM_Scale = m_Scale;
            String      finalM_OutPutPath = m_OutPutPath;
            int finalM_Thread = m_Thread;
            new Thread(() -> {
                Session m_JobSession = new Session(
                        finalM_Scale,
                        finalM_OutPutPath,
                        ".csv",
                        Optional.empty(),
                        "",
                        '|',
                        false,
                        false,
                        finalM_Thread,
                        chunkNumber,
                        true);
                TableGenerator tableGenerator = new TableGenerator(m_JobSession);
                tablesToGenerate.forEach(tableGenerator::generateTable);
            }).start();
        }
    }
}
