"""Query workload: each case has an equivalent Timeplus SQL and Elasticsearch request.

kind:
  count  -> both return one number (compared for correctness)
  groups -> both return {key: count} (compared for correctness)
  rows   -> both return up to N rows (row count compared; ties make ordering differ)

Placeholders: {trace} (a real trace id from the manifest), {since} / {since_iso}
(6 hours before the end of the dataset), {day_start} / {day_iso} (last day),
{hour_start} / {hour_iso} (last hour).
"""

def cases(p):
    def term(tok):
        return {"term": {"message": tok}}

    def flt(*clauses):
        return {"query": {"bool": {"filter": list(clauses)}}}

    def tp_count(where):
        return f"SELECT count() FROM table(app_logs) WHERE {where}"

    time6h = {"range": {"log_time": {"gte": p["since_iso"]}}}
    day = {"range": {"log_time": {"gte": p["day_iso"]}}}
    hour = {"range": {"log_time": {"gte": p["hour_iso"]}}}
    return [
        # --- selective token search (posting lists only) ---
        dict(id="q01_rare_token", group="token", kind="count",
             tp=tp_count("has_token(message, 'DeadlockDetected')"),
             es=flt(term("DeadlockDetected"))),
        dict(id="q02_common_token", group="token", kind="count",
             tp=tp_count("has_token(message, 'timeout')"),
             es=flt(term("timeout"))),
        dict(id="q03_and_two_tokens", group="token", kind="count",
             tp=tp_count("has_all_tokens(message, ['Connection', 'refused'])"),
             es=flt(term("Connection"), term("refused"))),
        dict(id="q04_or_two_tokens", group="token", kind="count",
             tp=tp_count("has_any_tokens(message, ['OutOfMemoryError', 'StackOverflowError'])"),
             es={"query": {"bool": {"filter": [{"terms": {"message": ["OutOfMemoryError", "StackOverflowError"]}}]}}}),
        dict(id="q05_trace_lookup", group="token", kind="count",
             tp=tp_count("has_token(message, '{trace}')".format(**p)),
             es=flt(term(p["trace"]))),
        dict(id="q06_no_hit", group="token", kind="count",
             tp=tp_count("has_token(message, 'ZzzNoSuchToken')"),
             es=flt(term("ZzzNoSuchToken"))),
        # --- token + structured filters ---
        dict(id="q07_token_level_time", group="combo", kind="count",
             tp=tp_count("has_token(message, 'refused') AND level = 'ERROR' AND log_time >= '{since}'".format(**p)),
             es=flt(term("refused"), {"term": {"level": "ERROR"}}, time6h)),
        dict(id="q08_latest_100", group="combo", kind="rows",
             tp="SELECT log_time, service, host, message FROM table(app_logs) "
                "WHERE has_token(message, 'NullPointerException') ORDER BY log_time DESC LIMIT 100",
             es={**flt(term("NullPointerException")), "size": 100, "sort": [{"log_time": "desc"}],
                 "_source": ["log_time", "service", "host", "message"]}),
        # --- search + analytics ---
        dict(id="q09_token_by_service", group="analytics", kind="groups",
             tp="SELECT service, count() FROM table(app_logs) WHERE has_token(message, 'timeout') GROUP BY service",
             es={**flt(term("timeout")), "size": 0, "aggs": {"g": {"terms": {"field": "service", "size": 100}}}}),
        dict(id="q10_token_hourly_last_day", group="analytics", kind="groups",
             tp="SELECT to_unix_timestamp(to_start_of_hour(log_time)) * 1000 AS h, count() FROM table(app_logs) "
                "WHERE has_any_tokens(message, ['refused', 'timeout']) AND log_time >= '{day_start}' GROUP BY h".format(**p),
             es={**flt({"terms": {"message": ["refused", "timeout"]}}, day), "size": 0,
                 "aggs": {"g": {"date_histogram": {"field": "log_time", "fixed_interval": "1h", "min_doc_count": 1}}}}),
        dict(id="q15_errors_by_host_service", group="analytics", kind="groups",
             tp="SELECT concat(host, '|', service) AS k, count() FROM table(app_logs) "
                "WHERE level IN ('ERROR', 'FATAL') GROUP BY k",
             es={**flt({"terms": {"level": ["ERROR", "FATAL"]}}), "size": 0,
                 "aggs": {"g": {"multi_terms": {"terms": [{"field": "host"}, {"field": "service"}],
                                                "size": 10000}}}}),
        dict(id="q16_level_by_minute_all", group="analytics", kind="groups",
             tp="SELECT to_unix_timestamp(to_start_of_minute(log_time)) * 1000 AS m, count() FROM table(app_logs) "
                "WHERE level != 'DEBUG' GROUP BY m",
             es={**{"query": {"bool": {"must_not": [{"term": {"level": "DEBUG"}}]}}}, "size": 0,
                 "aggs": {"g": {"date_histogram": {"field": "log_time", "fixed_interval": "1m", "min_doc_count": 1}}}}),
        # --- incident drill-down and dashboards: scoped by service / level / time, as people use logs ---
        dict(id="q17_incident_drilldown", group="incident", kind="rows",
             tp="SELECT log_time, host, message FROM table(app_logs) WHERE service = 'payment-service' "
                "AND level = 'ERROR' AND log_time >= '{hour_start}' AND has_token(message, 'timeout') "
                "ORDER BY log_time DESC LIMIT 50".format(**p),
             es={**flt({"term": {"service": "payment-service"}}, {"term": {"level": "ERROR"}}, hour, term("timeout")),
                 "size": 50, "sort": [{"log_time": "desc"}], "_source": ["log_time", "host", "message"]}),
        dict(id="q18_service_token_5min_day", group="incident", kind="groups",
             tp="SELECT to_unix_timestamp(to_start_of_interval(log_time, 5m)) * 1000 AS b, count() FROM table(app_logs) "
                "WHERE service = 'api-gateway' AND log_time >= '{day_start}' AND has_token(message, 'timeout') GROUP BY b".format(**p),
             es={**flt({"term": {"service": "api-gateway"}}, day, term("timeout")), "size": 0,
                 "aggs": {"g": {"date_histogram": {"field": "log_time", "fixed_interval": "5m", "min_doc_count": 1}}}}),
        dict(id="q19_errors_per_host_day", group="dashboard", kind="groups",
             tp="SELECT host, count() FROM table(app_logs) WHERE level IN ('ERROR', 'FATAL') "
                "AND log_time >= '{day_start}' GROUP BY host".format(**p),
             es={**flt({"terms": {"level": ["ERROR", "FATAL"]}}, day), "size": 0,
                 "aggs": {"g": {"terms": {"field": "host", "size": 1000}}}}),
        dict(id="q20_level_by_service_all", group="dashboard", kind="groups",
             tp="SELECT concat(service, '|', level) AS k, count() FROM table(app_logs) GROUP BY k",
             es={"size": 0, "aggs": {"g": {"multi_terms": {"terms": [{"field": "service"}, {"field": "level"}],
                                                          "size": 10000}}}}),
        dict(id="q21_top_services_by_token_day", group="dashboard", kind="groups",
             tp="SELECT service, count() FROM table(app_logs) WHERE log_time >= '{day_start}' "
                "AND has_any_tokens(message, ['timeout', 'refused', 'OutOfMemoryError']) GROUP BY service".format(**p),
             es={**flt({"terms": {"message": ["timeout", "refused", "OutOfMemoryError"]}}, day), "size": 0,
                 "aggs": {"g": {"terms": {"field": "service", "size": 100}}}}),
        # --- Chinese ---
        dict(id="q11_chinese_word", group="cjk", kind="count",
             tp=tp_count("has_token(message, '交易失败')"),
             es=flt(term("交易失败"))),
        dict(id="q12_chinese_and_stock", group="cjk", kind="count",
             tp=tp_count("has_all_tokens(message, ['风控拦截', '600519'])"),
             es=flt(term("风控拦截"), term("600519"))),
        dict(id="q13_chinese_substring", group="cjk", kind="count",
             tp=tp_count("message LIKE '%失败%'"),
             es=flt({"wildcard": {"message.wc": {"value": "*失败*"}}})),
        # --- substring ---
        dict(id="q14_substring", group="substring", kind="count",
             tp=tp_count("message LIKE '%imeout%'"),
             es=flt({"wildcard": {"message.wc": {"value": "*imeout*"}}})),
    ]
