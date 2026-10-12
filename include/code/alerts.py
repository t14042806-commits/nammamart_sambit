"""
include/code/alerts.py
---------------------------
Builds the end-of-run message ("did today's run work, and why not?") and sends it.

  1. Read this run's numbers from pipeline_runs and dq_results.
  2. Make a plain template message (always works, no LLM needed).
  3. If an LLM is configured (see llm.py) ask it to rewrite the message for
     the COO / store managers. Only counts and check names are sent to the LLM.
  4. Send to Slack if the SLACK_WEBHOOK_URL Variable is set, otherwise print to the log.
     (Uses the trainer's include/slack_notifier.py unchanged.)
"""

from include.code.llm import ask_llm
from include.code.warehouse import connect, ensure_audit_tables


def build_run_summary(dag_id, run_id):
    con = connect()
    try:
        ensure_audit_tables(con)
        run = con.execute(
            "SELECT status, rows_extracted, rows_loaded, rows_quarantined FROM pipeline_runs "
            "WHERE dag_id = ? AND run_id = ?", [dag_id, run_id]).fetchone()
        problems = con.execute(
            """
            SELECT check_name, severity, rows_failed
            FROM dq_results
            WHERE dag_id = ? AND run_id = ? AND status <> 'PASS'
            ORDER BY CASE severity WHEN 'Critical' THEN 0 ELSE 1 END, rows_failed DESC
            LIMIT 15
            """, [dag_id, run_id]).fetchall()
    finally:
        con.close()

    status, extracted, loaded, quarantined = run if run else ("unknown", 0, 0, 0)
    return {
        "dag_id": dag_id, "run_id": run_id, "status": status,
        "rows_extracted": extracted, "rows_loaded": loaded, "rows_quarantined": quarantined,
        "problems": [{"check": c, "severity": s, "rows": n} for c, s, n in problems],
    }


def template_message(summary):
    icon = "✅" if summary["status"] == "success" else "🚨"
    lines = [
        f"{icon} *{summary['dag_id']}* finished with status *{summary['status']}*",
        f"Run: {summary['run_id']}",
        f"Rows read: {summary['rows_extracted']} | loaded: {summary['rows_loaded']} "
        f"| quarantined: {summary['rows_quarantined']}",
    ]
    if summary["problems"]:
        lines.append("Top quality issues:")
        for p in summary["problems"][:8]:
            lines.append(f"  • {p['check']} ({p['severity']}): {p['rows']} rows")
    if summary["status"] != "success":
        lines.append("👉 Open the Airflow UI → Grid view → red task → Logs to see why.")
    return "\n".join(lines)


def build_and_send_alert(dag_id, run_id, webhook_url=None):
    summary = build_run_summary(dag_id, run_id)
    message = template_message(summary)

    llm_text = ask_llm(
        "Rewrite this data pipeline report as a short Slack message (max 6 lines) for a "
        "non-technical COO of a grocery delivery company. Say clearly whether today's "
        "numbers can be trusted and what the biggest data problem is. Keep the numbers exact.\n\n"
        + message
    )
    if llm_text:
        message = llm_text + "\n\n(raw numbers)\n" + template_message(summary)

    from include.slack_notifier import send_slack_alert
    send_slack_alert(message, webhook_url=webhook_url)
    return message
