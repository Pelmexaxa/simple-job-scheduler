/**
 * Парсер и форматировщик cron-выражений (5 полей, UTC).
 * Совместим с синтаксисом, который использует сервер (crate `cron`).
 */
const CronFormatter = (() => {
  const MESSAGES = {
    ru: {
      invalid_fields: 'Нужно ровно 5 полей: минута час день_месяца месяц день_недели',
      invalid_field: 'Некорректное поле «{field}»: {part}',
      every_minute: 'Каждую минуту',
      every_hour_at: 'Каждый час в :{min}',
      every_day_at: 'Каждый день в {time} UTC',
      every_weekday_at: 'Каждый {weekday} в {time} UTC',
      every_month_day_at: 'Каждое {day}-е число месяца в {time} UTC',
      every_n_minutes: 'Каждые {n} мин.',
      every_n_hours: 'Каждые {n} ч. (в :{min})',
      on_minutes: 'в минуты {list}',
      on_hours: 'в часы {list}',
      on_days: 'в числа {list}',
      on_months: 'в месяцы {list}',
      on_weekdays: 'по {list}',
      and: ' и ',
      at_time: 'в {time} UTC',
      field_minute: 'мин.',
      field_hour: 'час',
      field_dom: 'день',
      field_month: 'мес.',
      field_dow: 'день нед.',
      weekday_0: 'воскресенье',
      weekday_1: 'понедельник',
      weekday_2: 'вторник',
      weekday_3: 'среда',
      weekday_4: 'четверг',
      weekday_5: 'пятница',
      weekday_6: 'суббота',
      weekday_7: 'воскресенье',
      month_1: 'январь', month_2: 'февраль', month_3: 'март', month_4: 'апрель',
      month_5: 'май', month_6: 'июнь', month_7: 'июль', month_8: 'август',
      month_9: 'сентябрь', month_10: 'октябрь', month_11: 'ноябрь', month_12: 'декабрь',
      next_runs: 'Ближайшие запуски (UTC)',
      normalized: 'Нормализовано',
    },
    en: {
      invalid_fields: 'Exactly 5 fields required: minute hour day month weekday',
      invalid_field: 'Invalid field «{field}»: {part}',
      every_minute: 'Every minute',
      every_hour_at: 'Every hour at :{min}',
      every_day_at: 'Every day at {time} UTC',
      every_weekday_at: 'Every {weekday} at {time} UTC',
      every_month_day_at: 'On day {day} of each month at {time} UTC',
      every_n_minutes: 'Every {n} minutes',
      every_n_hours: 'Every {n} hours (at :{min})',
      on_minutes: 'at minutes {list}',
      on_hours: 'at hours {list}',
      on_days: 'on days {list}',
      on_months: 'in months {list}',
      on_weekdays: 'on {list}',
      and: ' and ',
      at_time: 'at {time} UTC',
      field_minute: 'min',
      field_hour: 'hour',
      field_dom: 'day',
      field_month: 'month',
      field_dow: 'weekday',
      weekday_0: 'Sunday',
      weekday_1: 'Monday',
      weekday_2: 'Tuesday',
      weekday_3: 'Wednesday',
      weekday_4: 'Thursday',
      weekday_5: 'Friday',
      weekday_6: 'Saturday',
      weekday_7: 'Sunday',
      month_1: 'January', month_2: 'February', month_3: 'March', month_4: 'April',
      month_5: 'May', month_6: 'June', month_7: 'July', month_8: 'August',
      month_9: 'September', month_10: 'October', month_11: 'November', month_12: 'December',
      next_runs: 'Upcoming runs (UTC)',
      normalized: 'Normalized',
    },
  };

  const FIELD_NAMES = ['minute', 'hour', 'day', 'month', 'weekday'];
  const FIELD_LIMITS = [
    { min: 0, max: 59 },
    { min: 0, max: 23 },
    { min: 1, max: 31 },
    { min: 1, max: 12 },
    { min: 0, max: 7 },
  ];

  function msg(lang, key, vars = {}) {
    const pack = MESSAGES[lang === 'ru' ? 'ru' : 'en'];
    let text = pack[key] || key;
    for (const [k, v] of Object.entries(vars)) {
      text = text.replace(`{${k}}`, v);
    }
    return text;
  }

  function normalize(expression) {
    if (!expression || typeof expression !== 'string') return '';
    return expression.trim().replace(/\s+/g, ' ');
  }

  function parseField(part, index) {
    const { min, max } = FIELD_LIMITS[index];
    const values = new Set();

    const addRange = (from, to, step = 1) => {
      for (let i = from; i <= to; i += step) {
        if (i >= min && i <= max) values.add(i);
      }
    };

    const parseAtom = (atom) => {
      if (atom === '*') {
        addRange(min, max);
        return;
      }
      if (atom.includes('/')) {
        const [base, stepStr] = atom.split('/');
        const step = parseInt(stepStr, 10);
        if (!step || step < 1) throw new Error('step');
        if (base === '*') {
          addRange(min, max, step);
        } else if (base.includes('-')) {
          const [a, b] = base.split('-').map((x) => parseInt(x, 10));
          addRange(a, b, step);
        } else {
          addRange(parseInt(base, 10), max, step);
        }
        return;
      }
      if (atom.includes('-')) {
        const [a, b] = atom.split('-').map((x) => parseInt(x, 10));
        addRange(a, b);
        return;
      }
      const n = parseInt(atom, 10);
      if (Number.isNaN(n)) throw new Error('number');
      if (n < min || n > max) throw new Error('range');
      values.add(n);
    };

    for (const piece of part.split(',')) {
      parseAtom(piece.trim());
    }

    if (values.size === 0) throw new Error('empty');
    return values;
  }

  function parse(expression) {
    const normalized = normalize(expression);
    const parts = normalized.split(' ').filter(Boolean);
    if (parts.length !== 5) {
      return { valid: false, error: 'fields', normalized, parts };
    }

    try {
      const fields = parts.map((p, i) => parseField(p, i));
      return { valid: true, normalized, parts, fields };
    } catch {
      return { valid: false, error: 'syntax', normalized, parts };
    }
  }

  function matchesField(values, value) {
    return values.has(value);
  }

  function matches(parsed, date) {
    const min = date.getUTCMinutes();
    const hour = date.getUTCHours();
    const dom = date.getUTCDate();
    const month = date.getUTCMonth() + 1;
    const dow = date.getUTCDay(); // 0=Sun

    const [mins, hours, doms, months, dows] = parsed.fields;

    const domMatch = matchesField(doms, dom);
    const dowMatch = matchesField(dows, dow) || matchesField(dows, dow === 0 ? 7 : dow);

    const dayMatch =
      doms.size === 31 && domMatch ||
      dows.size === 8 ||
      (parsed.parts[2] === '*' && parsed.parts[4] === '*') ||
      domMatch ||
      dowMatch;

    return (
      matchesField(mins, min) &&
      matchesField(hours, hour) &&
      matchesField(months, month) &&
      dayMatch
    );
  }

  function getNextRuns(expression, count = 3, from = new Date()) {
    const parsed = parse(expression);
    if (!parsed.valid) return [];

    const runs = [];
    const cursor = new Date(from);
    cursor.setUTCSeconds(0, 0);
    cursor.setUTCMilliseconds(0);
    cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);

    const limit = 525600 * 2;
    for (let i = 0; i < limit && runs.length < count; i++) {
      if (matches(parsed, cursor)) {
        runs.push(new Date(cursor));
      }
      cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);
    }
    return runs;
  }

  function pad(n) {
    return String(n).padStart(2, '0');
  }

  function formatTime(h, m) {
    return `${pad(h)}:${pad(m)}`;
  }

  function describeList(values, lang, type) {
    const pack = MESSAGES[lang === 'ru' ? 'ru' : 'en'];
    const sorted = [...values].sort((a, b) => a - b);
    if (type === 'weekday') {
      return sorted.map((d) => pack[`weekday_${d}`] || d).join(', ');
    }
    if (type === 'month') {
      return sorted.map((m) => pack[`month_${m}`] || m).join(', ');
    }
    return sorted.join(', ');
  }

  function isAll(values, min, max) {
    return values.size >= max - min + 1;
  }

  function describe(expression, lang = 'en') {
    const parsed = parse(expression);
    if (!parsed.valid) {
      if (parsed.error === 'fields') {
        return { valid: false, error: msg(lang, 'invalid_fields'), normalized: parsed.normalized };
      }
      const idx = parsed.parts?.findIndex((p, i) => {
        try { parseField(p, i); return false; } catch { return true; }
      }) ?? 0;
      return {
        valid: false,
        error: msg(lang, 'invalid_field', {
          field: msg(lang, `field_${FIELD_NAMES[idx]}`),
          part: parsed.parts?.[idx] || '',
        }),
        normalized: parsed.normalized,
      };
    }

    const [mins, hours, doms, months, dows] = parsed.fields;
    const m = msg;

    let description;

    if (isAll(mins, 0, 59) && isAll(hours, 0, 23) && parsed.parts[2] === '*' && parsed.parts[3] === '*' && parsed.parts[4] === '*') {
      description = m(lang, 'every_minute');
    } else if (mins.size === 1 && isAll(hours, 0, 23) && parsed.parts[2] === '*' && parsed.parts[3] === '*' && parsed.parts[4] === '*') {
      const min = [...mins][0];
      description = m(lang, 'every_hour_at', { min: pad(min) });
    } else if (mins.size === 1 && hours.size === 1 && parsed.parts[2] === '*' && parsed.parts[3] === '*' && parsed.parts[4] === '*') {
      const [hour] = [...hours];
      const [min] = [...mins];
      description = m(lang, 'every_day_at', { time: formatTime(hour, min) });
    } else if (mins.size === 1 && hours.size === 1 && dows.size === 1 && parsed.parts[2] === '*') {
      const [hour] = [...hours];
      const [min] = [...mins];
      const dow = [...dows][0];
      description = m(lang, 'every_weekday_at', {
        weekday: m(lang, `weekday_${dow}`),
        time: formatTime(hour, min),
      });
    } else if (mins.size === 1 && hours.size === 1 && doms.size === 1 && parsed.parts[4] === '*') {
      const [hour] = [...hours];
      const [min] = [...mins];
      const [day] = [...doms];
      description = m(lang, 'every_month_day_at', { day, time: formatTime(hour, min) });
    } else if (parsed.parts[0].startsWith('*/') && isAll(hours, 0, 23)) {
      const n = parsed.parts[0].slice(2);
      description = m(lang, 'every_n_minutes', { n });
    } else if (parsed.parts[1].startsWith('*/') && mins.size === 1) {
      const n = parsed.parts[1].slice(2);
      const [min] = [...mins];
      description = m(lang, 'every_n_hours', { n, min: pad(min) });
    } else {
      const bits = [];
      if (!isAll(mins, 0, 59)) bits.push(m(lang, 'on_minutes', { list: describeList(mins, lang) }));
      if (!isAll(hours, 0, 23)) bits.push(m(lang, 'on_hours', { list: describeList(hours, lang) }));
      if (parsed.parts[2] !== '*') bits.push(m(lang, 'on_days', { list: describeList(doms, lang) }));
      if (parsed.parts[3] !== '*') bits.push(m(lang, 'on_months', { list: describeList(months, lang, 'month') }));
      if (parsed.parts[4] !== '*') bits.push(m(lang, 'on_weekdays', { list: describeList(dows, lang, 'weekday') }));
      if (mins.size === 1 && hours.size === 1) {
        const [hour] = [...hours];
        const [min] = [...mins];
        bits.push(m(lang, 'at_time', { time: formatTime(hour, min) }));
      }
      description = bits.join(m(lang, 'and'));
    }

    return {
      valid: true,
      normalized: parsed.normalized,
      parts: parsed.parts,
      fieldLabels: [
        m(lang, 'field_minute'),
        m(lang, 'field_hour'),
        m(lang, 'field_dom'),
        m(lang, 'field_month'),
        m(lang, 'field_dow'),
      ],
      description,
      nextRuns: getNextRuns(parsed.normalized, 3),
    };
  }

  function format(expression, lang = 'en') {
    return describe(expression, lang);
  }

  return { normalize, parse, describe, format, getNextRuns, msg };
})();

if (typeof window !== 'undefined') {
  window.CronFormatter = CronFormatter;
}
