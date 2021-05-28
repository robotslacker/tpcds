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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SparkDriver
{
    public static void usage()
    {
        System.out.println("spark:   spark-submit --class io.trino.tpcds.Driver --master yarn tpcds-gen.jar OutPath Scale");
        System.out.println("         OutPath:  root directory of location to create data in.");
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

        // 检查是否在Spark运行环境中
        // 获得Scale的大小，单位是G，按照G来分配Worker数量，即每一个G由1个Worker来完成
        // 本地模式下，Worker的数量不超过CPU的数量
        SparkConf sparkConf = new SparkConf().setAppName("Spark TPCDS-Gen Application (java)");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        if (m_Thread == -1) {
            m_Thread = m_Scale;
            if (m_Thread > javaSparkContext.defaultParallelism() *4)
            {
                // 在Spark环境下，默认的Scale是Parallelism的4倍
                m_Thread = javaSparkContext.defaultParallelism() *4;
            }
        }

        List<Table> tablesToGenerate;
        tablesToGenerate = Table.getBaseTables();

        List<String> dataList = new ArrayList<>();
        for (int i = 1; i <= m_Thread; i++) {
            dataList.add(String.valueOf(i));
        }
        JavaRDD<String> jobs = javaSparkContext.parallelize(dataList, m_Thread);
        int         finalM_Scale = m_Scale;
        String      finalM_OutPutPath = m_OutPutPath;
        int finalM_Thread = m_Thread;
        long numTask = jobs.map
                (
                        (Function<String, Boolean>) s -> {
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
                                    Integer.parseInt(s),
                                    true);
                            TableGenerator tableGenerator = new TableGenerator(m_JobSession, true);
                            tablesToGenerate.forEach(tableGenerator::generateTable);
                            return true;
                        }
                ).count();
        System.out.println("Total " + numTask + " jobs finished.");
        //关闭context
        javaSparkContext.close();
    }
}
