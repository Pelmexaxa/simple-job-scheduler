//! Валидация тела запроса на создание/обновление задачи.

use crate::i18n::{LogLang, ValMsg};
use crate::models::{JOB_GROUP_MAX_LEN, JobInput, ScheduleType, normalize_job_group};
use crate::scheduler::parse_interval;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde_json::Value;
use std::str::FromStr;

/// Проверяет поля задачи; секции с выключенной галочкой не проверяются.
pub fn validate_job(input: &JobInput, lang: LogLang) -> Result<(), String> {
    if input.name.trim().is_empty() {
        return Err(ValMsg::NameRequired.text(lang).to_string());
    }

    validate_job_group(input, lang)?;

    validate_schedule(input, lang)?;

    if input.fetch_enabled {
        validate_fetch(input, lang)?;
    }

    if input.transform_enabled {
        validate_transform(input, lang)?;
    }

    if input.send_enabled {
        validate_send(input, lang)?;
    }

    if input.retry_enabled {
        validate_retry(input, lang)?;
    }

    Ok(())
}

fn validate_job_group(input: &JobInput, lang: LogLang) -> Result<(), String> {
    if let Some(group) = normalize_job_group(input.job_group.clone()) {
        if group.len() > JOB_GROUP_MAX_LEN {
            return Err(ValMsg::JobGroupInvalid.text(lang).to_string());
        }
        if group.chars().any(|c| c.is_control()) {
            return Err(ValMsg::JobGroupInvalid.text(lang).to_string());
        }
    }
    Ok(())
}

fn validate_schedule(input: &JobInput, lang: LogLang) -> Result<(), String> {
    let value = input.schedule_value.trim();
    if value.is_empty() {
        return Err(ValMsg::ScheduleValueRequired.text(lang).to_string());
    }

    match input.schedule_type {
        ScheduleType::Interval => match parse_interval(value) {
            Some(d) if d.num_seconds() > 0 => Ok(()),
            _ => Err(ValMsg::ScheduleIntervalInvalid.text(lang).to_string()),
        },
        ScheduleType::Cron => {
            if Schedule::from_str(value).is_ok() {
                Ok(())
            } else {
                Err(ValMsg::ScheduleCronInvalid.text(lang).to_string())
            }
        }
        ScheduleType::OneTime => match parse_one_time(value) {
            None => Err(ValMsg::ScheduleOneTimeInvalid.text(lang).to_string()),
            Some(at) if at <= Utc::now() => Err(ValMsg::ScheduleOneTimePast.text(lang).to_string()),
            Some(_) => Ok(()),
        },
    }
}

fn validate_fetch(input: &JobInput, lang: LogLang) -> Result<(), String> {
    let url = non_empty(input.fetch_url.as_ref())
        .ok_or_else(|| ValMsg::FetchUrlRequired.text(lang).to_string())?;
    if !is_http_url(url) {
        return Err(ValMsg::FetchUrlInvalid.text(lang).to_string());
    }

    let method = input
        .fetch_method
        .as_deref()
        .unwrap_or("GET")
        .trim()
        .to_uppercase();
    if method != "GET" && method != "POST" {
        return Err(ValMsg::FetchMethodInvalid.text(lang).to_string());
    }

    validate_json_object(
        input.fetch_headers.as_ref(),
        ValMsg::FetchHeadersInvalid,
        lang,
    )
}

fn validate_transform(input: &JobInput, lang: LogLang) -> Result<(), String> {
    if non_empty(input.transform_script.as_ref()).is_none() {
        return Err(ValMsg::TransformScriptRequired.text(lang).to_string());
    }
    Ok(())
}

fn validate_send(input: &JobInput, lang: LogLang) -> Result<(), String> {
    let url = non_empty(input.send_url.as_ref())
        .ok_or_else(|| ValMsg::SendUrlRequired.text(lang).to_string())?;
    if !is_http_url(url) {
        return Err(ValMsg::SendUrlInvalid.text(lang).to_string());
    }

    let method = input
        .send_method
        .as_deref()
        .unwrap_or("POST")
        .trim()
        .to_uppercase();
    if method != "POST" && method != "PUT" {
        return Err(ValMsg::SendMethodInvalid.text(lang).to_string());
    }

    validate_json_object(
        input.send_headers.as_ref(),
        ValMsg::SendHeadersInvalid,
        lang,
    )
}

fn validate_retry(input: &JobInput, lang: LogLang) -> Result<(), String> {
    match input.max_retries {
        Some(n) if n >= 0 => {}
        _ => return Err(ValMsg::MaxRetriesInvalid.text(lang).to_string()),
    }

    match input.retry_interval_seconds {
        Some(n) if n >= 1 => Ok(()),
        _ => Err(ValMsg::RetryIntervalInvalid.text(lang).to_string()),
    }
}

fn validate_json_object(raw: Option<&String>, msg: ValMsg, lang: LogLang) -> Result<(), String> {
    let Some(raw) = raw else {
        return Ok(());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    let value: Value = serde_json::from_str(trimmed).map_err(|_| msg.text(lang).to_string())?;
    if value.is_object() {
        Ok(())
    } else {
        Err(msg.text(lang).to_string())
    }
}

fn non_empty(value: Option<&String>) -> Option<&str> {
    value.and_then(|s| {
        let t = s.trim();
        if t.is_empty() { None } else { Some(t) }
    })
}

fn is_http_url(s: &str) -> bool {
    reqwest::Url::parse(s)
        .ok()
        .is_some_and(|u| matches!(u.scheme(), "http" | "https"))
}

fn parse_one_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|d| d.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ScheduleType;

    fn base_input() -> JobInput {
        JobInput {
            name: "Test".to_string(),
            job_group: None,
            description: None,
            enabled: true,
            schedule_type: ScheduleType::Interval,
            schedule_value: "5m".to_string(),
            fetch_enabled: false,
            fetch_method: None,
            fetch_url: None,
            fetch_headers: None,
            fetch_body: None,
            transform_enabled: false,
            transform_script: None,
            send_enabled: false,
            send_method: None,
            send_url: None,
            send_headers: None,
            send_body_template: None,
            retry_enabled: false,
            max_retries: None,
            retry_interval_seconds: None,
        }
    }

    #[test]
    fn rejects_empty_name() {
        let mut input = base_input();
        input.name = "  ".to_string();
        assert!(validate_job(&input, LogLang::En).is_err());
    }

    #[test]
    fn fetch_requires_url_when_enabled() {
        let mut input = base_input();
        input.fetch_enabled = true;
        input.fetch_url = Some(String::new());
        assert!(validate_job(&input, LogLang::En).is_err());
    }

    #[test]
    fn skips_fetch_when_disabled() {
        let mut input = base_input();
        input.fetch_url = None;
        assert!(validate_job(&input, LogLang::En).is_ok());
    }
}
