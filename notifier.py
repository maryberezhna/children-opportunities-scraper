"""notifier.py — daily email report via Gmail SMTP.

Set these env vars / GitHub Secrets:
  GMAIL_APP_PASSWORD  — 16-char Gmail App Password (myaccount.google.com/apppasswords)
  GMAIL_FROM          — sender address (default: mashaberezhna0209@gmail.com)
"""
import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

GMAIL_FROM = os.environ.get("GMAIL_FROM", "mashaberezhna0209@gmail.com")
GMAIL_TO = "mashaberezhna0209@gmail.com"
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

_TYPE_EMOJI = {
    "course": "📚", "olympiad": "🏆", "competition": "🥇",
    "club": "🎭", "exchange": "✈️", "camp": "⛺",
    "scholarship": "🎓", "grant": "💰", "allowance": "💰",
    "festival": "🎪", "medical_aid": "🏥", "psychology": "🧠",
    "rehabilitation": "🏋️", "humanitarian": "🤲",
    "internship": "💼", "volunteer": "🌿",
}

_STATUS_COLOR = {"success": "#16a34a", "error": "#dc2626", "empty": "#d97706"}
_STATUS_LABEL = {"success": "✅ ок", "error": "❌ помилка", "empty": "⚠️ порожньо"}


def send_daily_report(
    new_opportunities: list[dict],
    health: dict,
    results: list[dict],
    archived: int,
) -> bool:
    """Send the daily scraper report. Returns True if sent successfully."""
    if not GMAIL_APP_PASSWORD:
        logger.warning("GMAIL_APP_PASSWORD not set — skipping daily email")
        return False

    today = datetime.now().strftime("%Y-%m-%d")
    n_new = len(new_opportunities)
    subject = f"🎓 Dityam — {today}: {n_new} записів збережено"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_FROM
    msg["To"] = GMAIL_TO
    msg.attach(MIMEText(_build_text(today, new_opportunities, health, results, archived), "plain", "utf-8"))
    msg.attach(MIMEText(_build_html(today, new_opportunities, health, results, archived), "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_FROM, GMAIL_APP_PASSWORD)
            smtp.sendmail(GMAIL_FROM, [GMAIL_TO], msg.as_string())
        logger.info("Daily report sent: %s", subject)
        return True
    except Exception as e:
        logger.error("Failed to send email: %s", e)
        return False


# ── HTML builder ──────────────────────────────────────────────────────────────

def _build_html(today, new_opps, health, results, archived):
    total_active = health.get("total_active", 0)
    total_archived = health.get("total_archived", 0)
    no_deadline = health.get("no_deadline", 0)
    n_new = len(new_opps)
    errors = [r for r in results if r["status"] == "error"]

    # ── new opportunities table ───────────────────────────────────────────────
    if new_opps:
        rows = ""
        for op in new_opps:
            emoji = _TYPE_EMOJI.get(op.get("opportunity_type", ""), "🔹")
            age = _age_range(op)
            deadline = op.get("deadline") or "—"
            title_link = (
                f'<a href="{op["source_url"]}" style="color:#1a56db;text-decoration:none;">'
                f'{_esc(op.get("title", "—"))}</a>'
                if op.get("source_url") else _esc(op.get("title", "—"))
            )
            rows += (
                f'<tr style="border-bottom:1px solid #f0f0f0;">'
                f'<td style="padding:8px 12px;">{emoji} {title_link}</td>'
                f'<td style="padding:8px 12px;color:#555;font-size:13px;">{_esc(op.get("source",""))}</td>'
                f'<td style="padding:8px 12px;color:#555;font-size:13px;">{age}</td>'
                f'<td style="padding:8px 12px;color:#555;font-size:13px;">{deadline}</td>'
                f'</tr>'
            )
        new_section = f"""
        <h2 style="font-size:16px;color:#111;margin:24px 0 12px;">
          📋 Збережено сьогодні ({n_new})
        </h2>
        <table style="width:100%;border-collapse:collapse;background:white;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08);">
          <thead>
            <tr style="background:#f8fafc;">
              <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Назва</th>
              <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Джерело</th>
              <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Вік</th>
              <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Дедлайн</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>"""
    else:
        new_section = """
        <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:16px;color:#92400e;font-size:14px;">
          ℹ️ Сьогодні скрапери не повернули жодного запису.
        </div>"""

    # ── scraper results table ─────────────────────────────────────────────────
    scraper_rows = ""
    for r in results:
        color = _STATUS_COLOR.get(r["status"], "#555")
        label = _STATUS_LABEL.get(r["status"], r["status"])
        count = r.get("count", 0)
        dur = r.get("duration", 0)
        err_msg = f'<br><span style="color:#dc2626;font-size:12px;">{_esc(r.get("error",""))}</span>' if r.get("error") else ""
        scraper_rows += (
            f'<tr style="border-bottom:1px solid #f0f0f0;">'
            f'<td style="padding:8px 12px;font-weight:500;">{_esc(r["name"])}</td>'
            f'<td style="padding:8px 12px;color:{color};font-size:13px;">{label}{err_msg}</td>'
            f'<td style="padding:8px 12px;color:#555;font-size:13px;">{count}</td>'
            f'<td style="padding:8px 12px;color:#888;font-size:13px;">{dur:.1f}s</td>'
            f'</tr>'
        )

    # ── errors banner ─────────────────────────────────────────────────────────
    error_banner = ""
    if errors:
        names = ", ".join(r["name"] for r in errors)
        error_banner = f"""
        <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:14px;margin-bottom:16px;color:#991b1b;font-size:14px;">
          ❌ <strong>Помилки скраперів:</strong> {_esc(names)}<br>
          <span style="font-size:12px;">Перевірте GitHub Actions для деталей.</span>
        </div>"""

    # ── health warnings ───────────────────────────────────────────────────────
    warnings = []
    if no_deadline > total_active * 0.8:
        warnings.append(f"⚠️ {no_deadline} з {total_active} активних записів не мають дедлайну")
    if warnings:
        warning_html = "<br>".join(warnings)
        health_warn = f"""
        <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:14px;margin-top:16px;color:#92400e;font-size:13px;">
          {warning_html}
        </div>"""
    else:
        health_warn = ""

    return f"""<!DOCTYPE html>
<html lang="uk">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:20px;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif;color:#333;">
<div style="max-width:680px;margin:0 auto;">

  <!-- Header -->
  <div style="background:#1e40af;color:white;padding:24px 28px;border-radius:10px 10px 0 0;">
    <h1 style="margin:0;font-size:20px;font-weight:700;">🎓 Dityam Scrapers</h1>
    <p style="margin:6px 0 0;opacity:.8;font-size:14px;">Щоденний звіт — {today}</p>
  </div>

  <!-- Stats row -->
  <div style="background:white;padding:20px 28px;display:flex;gap:12px;border-bottom:1px solid #e5e7eb;">
    <div style="flex:1;text-align:center;padding:14px;background:#eff6ff;border-radius:8px;">
      <div style="font-size:26px;font-weight:700;color:#1e40af;">{n_new}</div>
      <div style="color:#6b7280;font-size:12px;margin-top:2px;">збережено сьогодні</div>
    </div>
    <div style="flex:1;text-align:center;padding:14px;background:#f0fdf4;border-radius:8px;">
      <div style="font-size:26px;font-weight:700;color:#15803d;">{total_active}</div>
      <div style="color:#6b7280;font-size:12px;margin-top:2px;">активних у базі</div>
    </div>
    <div style="flex:1;text-align:center;padding:14px;background:#fff7ed;border-radius:8px;">
      <div style="font-size:26px;font-weight:700;color:#c2410c;">{archived}</div>
      <div style="color:#6b7280;font-size:12px;margin-top:2px;">архівовано</div>
    </div>
    <div style="flex:1;text-align:center;padding:14px;background:#f5f3ff;border-radius:8px;">
      <div style="font-size:26px;font-weight:700;color:#6d28d9;">{total_archived}</div>
      <div style="color:#6b7280;font-size:12px;margin-top:2px;">всього архів</div>
    </div>
  </div>

  <!-- Body -->
  <div style="background:white;padding:24px 28px;border-radius:0 0 10px 10px;box-shadow:0 1px 4px rgba(0,0,0,.06);">

    {error_banner}

    {new_section}

    <h2 style="font-size:16px;color:#111;margin:28px 0 12px;">🕷️ Результати скраперів</h2>
    <table style="width:100%;border-collapse:collapse;">
      <thead>
        <tr style="background:#f8fafc;">
          <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Скрапер</th>
          <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Статус</th>
          <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Записів</th>
          <th style="padding:10px 12px;text-align:left;font-size:13px;color:#666;font-weight:600;">Час</th>
        </tr>
      </thead>
      <tbody>{scraper_rows}</tbody>
    </table>

    <h2 style="font-size:16px;color:#111;margin:28px 0 12px;">🗄️ Здоров'я бази</h2>
    <table style="width:100%;border-collapse:collapse;">
      <tr><td style="padding:6px 0;color:#555;font-size:14px;">Активних записів</td><td style="padding:6px 0;font-weight:600;">{total_active}</td></tr>
      <tr><td style="padding:6px 0;color:#555;font-size:14px;">Архівованих записів</td><td style="padding:6px 0;font-weight:600;">{total_archived}</td></tr>
      <tr><td style="padding:6px 0;color:#555;font-size:14px;">Без дедлайну (активні)</td><td style="padding:6px 0;font-weight:600;">{no_deadline}</td></tr>
    </table>
    {health_warn}

  </div>

  <p style="text-align:center;color:#9ca3af;font-size:12px;margin-top:16px;">
    Dityam.com.ua — автоматичний звіт · {today}
  </p>

</div>
</body>
</html>"""


# ── plain-text fallback ───────────────────────────────────────────────────────

def _build_text(today, new_opps, health, results, archived):
    lines = [
        f"🎓 Dityam Scrapers — {today}",
        "=" * 50,
        f"Збережено сьогодні: {len(new_opps)}",
        f"Активних у базі: {health.get('total_active', 0)}",
        f"Архівовано сьогодні: {archived}",
        "",
    ]

    if new_opps:
        lines.append("НОВІ МОЖЛИВОСТІ:")
        for op in new_opps:
            age = _age_range(op)
            deadline = op.get("deadline") or "без дедлайну"
            lines.append(f"  • {op.get('title','—')} [{op.get('source','')}] вік {age} | {deadline}")
            if op.get("source_url"):
                lines.append(f"    {op['source_url']}")
    else:
        lines.append("Нових можливостей сьогодні не додано.")

    lines += ["", "СКРАПЕРИ:"]
    for r in results:
        status = {"success": "OK", "error": "ПОМИЛКА", "empty": "ПОРОЖНЬО"}[r["status"]]
        lines.append(f"  {r['name']:25s} {status:10s} {r.get('count',0):3d} записів  {r.get('duration',0):.1f}s")
        if r.get("error"):
            lines.append(f"    → {r['error']}")

    lines += [
        "",
        "БАЗА:",
        f"  Активних: {health.get('total_active', 0)}",
        f"  Архів:    {health.get('total_archived', 0)}",
        f"  Без дедлайну: {health.get('no_deadline', 0)}",
    ]
    return "\n".join(lines)


# ── helpers ───────────────────────────────────────────────────────────────────

def _age_range(op: dict) -> str:
    a, b = op.get("age_from"), op.get("age_to")
    if a is not None and b is not None:
        return f"{a}–{b} р."
    if a is not None:
        return f"{a}+ р."
    if b is not None:
        return f"до {b} р."
    return "—"


def _esc(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
