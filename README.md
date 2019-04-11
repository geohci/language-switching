# Language Switching Analysis

Webrequest dataset created through the following Hive query:

    SELECT reflect('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', CONCAT(user_agent, client_ip, "{SALT}")) AS user,
           concat(translate(normalized_host.project, '-', '_'), 'wiki') AS project,
           COALESCE(pageview_info["page_title"], "EDITATTEMPT") as page_title,
           page_id AS page_id,
           dt,
           geocoded_data['country'] AS country
      FROM wmf.webrequest 
     WHERE normalized_host.project_family = "wikipedia"
           AND ((is_pageview AND namespace_id = 0)
                OR (uri_query LIKE '%action=edit%' OR uri_query LIKE '%action=visualeditor%'
                    OR uri_query LIKE '%&intestactions=edit&intestactionsdetail=full&uiprop=options%'))
           AND agent_type = "user" 
           AND year = {YEAR} AND month = {MONTH} AND day = {DAY}
           AND SUBSTR(ip, -1, 1) = {# FROM 0-9}'''
           
The following WHERE clauses capture:

    (is_pageview AND namespace_id = 0) => page views to articles
    (uri_query LIKE '%action=edit%') => desktop wikitext editor
    (uri_query LIKE '%action=visualeditor%') => desktop and mobile visualeditor
    (uri_query LIKe '%&intestactions=edit&intestactionsdetail=full&uiprop=options%') => mobile wikitext editor

           
Wikidata items (Q######) are then joined based by joining on project and page_id and the table is exported to a TSV file.

Scripts:
* desc_stats.py: basic descriptive statistics regarding user sessions and language switching
* session_utils.py: utils for converting page views into sessions and identifying (non)-language switches
* lda_predictive_model.py: proof-of-concept test with logistic regression and LDA topic model for predicting language switches.
* test_switches.py: make sure language switching identification works as expected