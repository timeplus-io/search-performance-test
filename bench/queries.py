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
        # --- log analysis: regex, grok, percentiles, distinct, patterns, joins (ES side in ES|QL) ---
        dict(id="q22_regex_5xx_count", group="parse", kind="count",
             tp=tp_count("match(message, 'status=5[0-9]{2} ')"),
             es=flt({"regexp": {"message.wc": {"value": ".*status=5[0-9]{2} .*"}}})),
        dict(id="q23_regex_latency_sum_by_service", group="parse", kind="groups",
             tp="SELECT service, sum(to_int64(extract(message, 'latency_ms=([0-9]+)'))) FROM table(app_logs) "
                "WHERE has_token(message, 'latency') AND match(message, 'status=5[0-9]{2} ') GROUP BY service",
             esql='FROM app_logs | WHERE MATCH(message, "latency") AND message RLIKE ".*status=5[0-9]{2} .*" '
                  '| GROK message "latency_ms=%{INT:lat:int}" | STATS s = SUM(lat) BY service | KEEP service, s'),
        dict(id="q24_grok_requests_by_status", group="parse", kind="groups",
             tp="SELECT grok(message, 'GET %{URIPATH:path} status=%{INT:status} latency_ms=%{INT:lat} trace=%{WORD:trace}%{GREEDYDATA:rest}')['status'] AS st, "
                "count() FROM table(app_logs) WHERE has_token(message, 'latency') GROUP BY st",
             esql='FROM app_logs | WHERE MATCH(message, "latency") '
                  '| GROK message "GET %{URIPATH:path} status=%{INT:status} latency_ms=%{INT:lat} trace=%{WORD:trace}%{GREEDYDATA:rest}" '
                  '| STATS c = COUNT(*) BY status | KEEP status, c'),
        dict(id="q25_slow_requests_per_host", group="parse", kind="groups",
             tp="SELECT host, count() FROM table(app_logs) WHERE has_token(message, 'latency') "
                "AND to_int32(extract(message, 'latency_ms=([0-9]+)')) > 2500 GROUP BY host",
             esql='FROM app_logs | WHERE MATCH(message, "latency") | GROK message "latency_ms=%{INT:lat:int}" '
                  '| WHERE lat > 2500 | STATS c = COUNT(*) BY host | KEEP host, c'),
        dict(id="q26_p95_latency_by_service", group="parse", kind="groups", tolerance=0.03,
             tp="SELECT service, quantile(to_int32(extract(message, 'latency_ms=([0-9]+)')), 0.95) FROM table(app_logs) "
                "WHERE has_token(message, 'latency') GROUP BY service",
             esql='FROM app_logs | WHERE MATCH(message, "latency") | GROK message "latency_ms=%{INT:lat:int}" '
                  '| STATS p = PERCENTILE(lat, 95) BY service | KEEP service, p'),
        dict(id="q27_unique_users_per_day", group="parse", kind="groups", tolerance=0.02,
             tp="SELECT to_unix_timestamp(to_start_of_day(log_time)) * 1000 AS d, "
                "count_distinct(extract(message, 'user ([0-9]+) login')) FROM table(app_logs) "
                "WHERE has_token(message, 'login') GROUP BY d",
             esql='FROM app_logs | WHERE MATCH(message, "login") | DISSECT message "user %{uid} login %{rest}" '
                  '| EVAL d = TO_LONG(DATE_TRUNC(1 day, log_time)) | STATS u = COUNT_DISTINCT(uid, 40000) BY d | KEEP d, u'),
        dict(id="q28_error_patterns", group="parse", kind="groups",
             tp="SELECT replace_regex(message, '[0-9a-f]{8,}|[0-9]+', '#') AS pat, count() AS c FROM table(app_logs) "
                "WHERE level IN ('ERROR', 'FATAL') GROUP BY pat HAVING c > 1000",
             esql='FROM app_logs | WHERE level IN ("ERROR", "FATAL") '
                  '| EVAL pat = REPLACE(message, "[0-9a-f]{8,}|[0-9]+", "#") | STATS c = COUNT(*) BY pat '
                  '| WHERE c > 1000 | KEEP pat, c'),
        dict(id="q29_error_ratio_by_service_day", group="agg", kind="groups",
             tp="SELECT service, round(count_if(level IN ('ERROR', 'FATAL')) / count(), 4) FROM table(app_logs) "
                "WHERE log_time >= '{day_start}' GROUP BY service".format(**p),
             esql='FROM app_logs | WHERE log_time >= "{day_iso}" | EVAL e = CASE(level IN ("ERROR", "FATAL"), 1, 0) '
                  '| STATS errors = SUM(e), total = COUNT(*) BY service '
                  '| EVAL r = ROUND(TO_DOUBLE(errors) / total, 4) | KEEP service, r'.format(**p)),
        dict(id="q30_join_errors_by_team", group="join", kind="groups",
             tp="SELECT o.team, count() FROM table(app_logs) AS l INNER JOIN table(service_owner) AS o "
                "ON l.service = o.service WHERE l.level IN ('ERROR', 'FATAL') GROUP BY o.team",
             esql='FROM app_logs | WHERE level IN ("ERROR", "FATAL") | LOOKUP JOIN service_owner ON service '
                  '| STATS c = COUNT(*) BY team | KEEP team, c'),
        dict(id="q31_join_errors_after_deploy", group="join", kind="groups",
             tp="SELECT d.service, count() FROM table(app_logs) AS l INNER JOIN table(deployments) AS d "
                "ON l.service = d.service WHERE l.level IN ('ERROR', 'FATAL') AND l.log_time >= '{day_start}' "
                "AND l.log_time >= d.deploy_time AND l.log_time < d.deploy_time + INTERVAL 15 MINUTE "
                "GROUP BY d.service".format(**p),
             esql='FROM app_logs | WHERE level IN ("ERROR", "FATAL") AND log_time >= "{day_iso}" '
                  '| LOOKUP JOIN deployments ON service '
                  '| WHERE log_time >= deploy_time AND log_time < deploy_time + 15 minutes '
                  '| STATS c = COUNT(*) BY service | KEEP service, c'.format(**p)),
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
