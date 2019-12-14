forex_history = LOAD 'hdfs://hadoop-master:54310/user/hduser/market/EURUSD_GBP_CHF.csv'

USING
PigStorage(',') as (eurusd_date:chararray, eurusd_time:chararray, eurusd_open:float, eurusd_max:float, eurusd_min:float, eurusd_close:float, eurusd_volume:float,
eurgbp_date:chararray, eurgbp_time:chararray, eurgbp_open:float, eurgbp_max:float, eurgbp_min:float, eurgbp_close:float, eurgbp_volume:float,
eurchf_date:chararray, eurchf_time:chararray, eurchf_open:float, eurchf_max:float, eurchf_min:float, eurchf_close:float, eurchf_volume:float);

eurusd_data = FOREACH forex_history GENERATE SUBSTRING(eurusd_date, 0, 4) as eurusd_year, eurusd_open;
eurgbp_data = FOREACH forex_history GENERATE SUBSTRING(eurgbp_date, 0, 4) as eurgbp_year, eurgbp_open;

eurusd_group = GROUP eurusd_data BY eurusd_year;
eurgbp_group = GROUP eurgbp_data BY eurgbp_year;

eurusd_max_by_year = FOREACH eurusd_group GENERATE group as eurusd_year, MAX(eurusd_data.eurusd_open);
eurgbp_max_by_year = FOREACH eurgbp_group GENERATE group as eurgbp_year, MAX(eurgbp_data.eurgbp_open);

result = UNION eurusd_max_by_year, eurgbp_max_by_year;

STORE result INTO 'hdfs://hadoop-master:54310/user/hduser/results/vlisovyi_lab3fixed' USING PigStorage (',');