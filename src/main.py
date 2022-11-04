import os
import sys
from pathlib import Path
import logging
import warnings
from pyspark.sql import  SparkSession

warnings.filterwarnings("ignore")
sys.path.append(str(Path.cwd().parent))
logging.basicConfig(filename='log.log', filemode='w',
                    format='%(asctime)s.%(msecs)03d : %(name)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
logging.getLogger().setLevel(logging.DEBUG)

import env

if __name__ == "__main__":
    try:
        star_str = "*"*10
        exec(open(r"{}/{}".format(os.getcwd(), env.isi_py_file)).read())
        logging.info("{} Increase File Task Completed {}".format(star_str, star_str))
        # exec(open(r"{}/{}".format(os.getcwd(), env.kss_py_file)).read())
        logging.info("{} Kafka to S3 Task Completed {}".format(star_str, star_str))
        exec(open(r"{}/{}".format(os.getcwd(), env.rl_py_file)).read())
        logging.info("{} Raw Layer Task Completed {}".format(star_str, star_str))
        exec(open(r"{}/{}".format(os.getcwd(), env.cll_py_file)).read())
        logging.info("{} Cleansed Layer Task Completed {}".format(star_str, star_str))
        exec(open(r"{}/{}".format(os.getcwd(), env.cul_py_file)).read())
        logging.info("{} Curated Layer Task Completed {}".format(star_str, star_str))
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        print("raw_log_details :")
        spark.sql("select count(*) from log.raw_log_details").show()
        print("**************")
        print("cleansed_log_details :")
        spark.sql("select count(*) from log.cleansed_log_details").show()
        print("**************")
        print("curated_log_details :")
        spark.sql("select count(*) from log.curated_log_details").show()
        print("**************")
        print("log_agg_per_device :")
        spark.sql("select count(*) from log.log_agg_per_device").show()
        print("**************")
        print("log_agg_across_device :")
        spark.sql("select count(*) from log.log_agg_across_device").show()
        print("**************")
    except Exception as e:
        logging.exception('')
        sys.exit(1)
