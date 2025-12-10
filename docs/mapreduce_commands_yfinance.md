# Yahoo Finance MapReduce Command Cheatsheet (Mark Chen)

All commands assume you are logged into the course Hadoop edge node. Replace `$USER` with your NetID.

## 1. Upload Raw Extracts

```bash
hdfs dfs -mkdir -p /user/$USER/rbda/yfinance/raw
hdfs dfs -put -f market_data/yfinance/yfinance_equity.csv /user/$USER/rbda/yfinance/raw/
hdfs dfs -put -f market_data/yfinance/yfinance_rates.csv /user/$USER/rbda/yfinance/raw/
hdfs dfs -put -f market_data/yfinance/yfinance_vix.csv /user/$USER/rbda/yfinance/raw/
```

## 2. Run Profiling MapReduce (Hadoop Streaming)

```bash
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -D mapreduce.job.name="yf_profile_equity" \
  -input  /user/$USER/rbda/yfinance/raw/yfinance_equity.csv \
  -output /user/$USER/rbda/yfinance/profile_equity \
  -files mapreduce/yfinance_profile_mapper.py,mapreduce/yfinance_profile_reducer.py \
  -mapper  "python3 yfinance_profile_mapper.py" \
  -reducer "python3 yfinance_profile_reducer.py"
```

Repeat for `yfinance_rates.csv`/`yfinance_vix.csv` as needed by adjusting the input/output paths (e.g., `profile_rates`).

Inspect the results:

```bash
hdfs dfs -cat /user/$USER/rbda/yfinance/profile_equity/part-* | head -n 20
```

## 3. Run Cleaning/Deduping MapReduce

```bash
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -D mapreduce.job.name="yf_clean_equity" \
  -input  /user/$USER/rbda/yfinance/raw/yfinance_equity.csv \
  -output /user/$USER/rbda/yfinance/clean_equity \
  -files mapreduce/yfinance_clean_mapper.py,mapreduce/yfinance_clean_reducer.py \
  -mapper  "python3 yfinance_clean_mapper.py" \
  -reducer "python3 yfinance_clean_reducer.py"
```

Check the cleaned sample and pull it back if needed:

```bash
hdfs dfs -cat /user/$USER/rbda/yfinance/clean_equity/part-* | head -n 20
hdfs dfs -getmerge /user/$USER/rbda/yfinance/clean_equity cleaned_yfinance_equity.csv
```

## 4. Capture Command Log for Submission

Save your exact shell commands (with timestamps) to include in the assignment:

```bash
script -f mark_yfinance_commands.log   # start recording
# ...run the commands above...
exit                                   # stop recording
```

This log, along with the MapReduce code and PDF report, forms the deliverable for Mark's checkpoint submission.
