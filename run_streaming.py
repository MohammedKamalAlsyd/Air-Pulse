import os
import subprocess
import sys

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

# The path to your streaming job inside the container or locally
# Since docker-compose mounts ./jobs to /opt/spark/jobs:
JOB_FILE_PATH = "/opt/spark/jobs/streaming.py"

# Maven Coordinates for Spark 4.1.0 (Scala 2.13) and Hadoop AWS
PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
    "org.apache.hadoop:hadoop-aws:3.4.2",
    "com.amazonaws:aws-java-sdk-s3:1.12.796"
]

# The name of the Spark Master container in docker-compose
SPARK_MASTER_CONTAINER = "spark-master"
SPARK_MASTER_URL = "spark://spark-master:7077"

def run_spark_job():
    """
    Submits the Spark job to the Docker container.
    """
    packages_arg = ",".join(PACKAGES)
    
    # Construct the docker exec command
    # We execute spark-submit INSIDE the spark-master container
    # so it has access to the internal network (broker:29092)
    cmd = [
        "docker", "exec", "-it", SPARK_MASTER_CONTAINER,
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER_URL,
        "--packages", packages_arg,
        "--conf", "spark.jars.ivy=/tmp/.ivy",
        "--conf", "spark.driver.extraJavaOptions=-Duser.timezone=UTC",
        "--conf", "spark.executor.extraJavaOptions=-Duser.timezone=UTC",
        JOB_FILE_PATH
    ]

    print(f"Submitting job to container '{SPARK_MASTER_CONTAINER}'...")
    print(f"Command: {' '.join(cmd)}\n")

    try:
        # Check if config.py has valid AWS credentials before running
        # (This is a simplified check, assumes config.py is locally available to check)
        if os.path.exists("jobs/config.py"):
            with open("jobs/config.py", "r") as f:
                content = f.read()
                if "your_access_key_id" in content or "your_secret_access_key" in content:
                    print("⚠️  WARNING: It looks like 'jobs/config.py' still has placeholder credentials.")
                    print("   Ensure you have updated AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
                    confirm = input("   Continue anyway? (y/n): ")
                    if confirm.lower() != 'y':
                        sys.exit(0)

        # Run the subprocess
        subprocess.run(cmd, check=True)

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error submitting Spark job: {e}")
    except KeyboardInterrupt:
        print("\n🛑 Job submission cancelled.")

if __name__ == "__main__":
    run_spark_job()