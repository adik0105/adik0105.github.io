# web_compare.py
import os
import json
from io import BytesIO
from pathlib import Path
from flask import Flask, render_template_string, request, send_file, redirect, url_for
import pandas as pd
import requests
import tempfile

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200 MB limit

OUTPUT_XLSX = Path("comparison_result.xlsx")

HTML = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>WEB Проверятор Интентов</title>
  <style>
    body{font-family:Arial,Helvetica,sans-serif;margin:20px;background:#f6f7fb}
    .card{background:#fff;padding:18px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.06);max-width:980px;margin:auto}
    label{display:block;margin-top:10px;font-weight:600}
    input[type=text], input[type=file]{width:100%;padding:8px;margin-top:6px;border:1px solid #ddd;border-radius:6px}
    .row{display:flex;gap:10px}
    .col{flex:1}
    button{padding:10px 16px;margin-top:12px;border:0;border-radius:8px;cursor:pointer}
    .btn-primary{background:#2563eb;color:white}
    .btn-ghost{background:#e6eefc;color:#2563eb}
    .result{margin-top:18px}
    table{border-collapse:collapse;width:100%;margin-top:8px}
    table, th, td {border:1px solid #ddd}
    th, td{padding:6px;text-align:left;font-size:13px}
    .controls{display:flex;gap:8px;margin-top:10px}
    a.download{display:inline-block;margin-top:8px}
    pre.err{background:#fee;padding:8px;border-radius:6px;color:#900}
  </style>
</head>
<body>
  <div class="card">
    <h2>WEB Проверятор Интентов</h2>
    <form method="post" enctype="multipart/form-data">
      <label>JSON 1 (файл)</label>
      <input type="file" name="json1">
      <label>JSON 2 (файл)</label>
      <input type="file" name="json2">
      <label>JSON 3 (файл)</label>
      <input type="file" name="json3">
      <label>JSON 4 (файл)</label>
      <input type="file" name="json4">

      <div style="height:8px"></div>

      <label>Excel таблица (.xlsx) — либо загрузить файл, либо вставить ссылку Google Sheets</label>
      <input type="file" name="excel_file">
      <label style="font-weight:400;margin-top:6px">ИЛИ Google Sheets ссылка (вставь ссылку):</label>
      <input type="text" name="excel_url" placeholder="https://docs.google.com/spreadsheets/d/.../edit#gid=...">

      <div class="controls">
        <button class="btn-primary" name="action" value="compare_2">(2) Сравнить 2 файла</button>
        <button class="btn-primary" name="action" value="compare_4">(4) Сравнить 4 файла</button>
        <button class="btn-ghost" name="action" value="clear">Очистить форму</button>
      </div>
    </form>

    {% if error %}
      <div class="result"><pre class="err">{{ error }}</pre></div>
    {% endif %}

    {% if results_html %}
      <div class="result">
        <h3>Результат</h3>
        {{ results_html|safe }}
        <div>
          <a class="download" href="{{ url_for('download') }}">⬇️ Скачать результат (.xlsx)</a>
        </div>
      </div>
    {% endif %}
  </div>
</body>
</html>
"""

# -------------------- Utilities --------------------

def fix_gsheet_url(url: str) -> str:
    """
    Преобразует типичную ссылку Google Sheets в экспортную ссылку xlsx.
    Если уже экспортная — вернёт как есть.
    """
    if not url:
        return url
    url = url.strip()
    # Если уже pub?output=xlsx
    if "export?format=xlsx" in url or "output=xlsx" in url:
        # заменим двойные ? на &
        url = url.replace("?format=xlsx?", "?format=xlsx&")
        return url
    # стандартный edit URL
    if "/d/" in url:
        try:
            sheet_id = url.split("/d/")[1].split("/")[0]
            gid = None
            if "gid=" in url:
                gid = url.split("gid=")[1].split("&")[0].split("#")[0]
            if gid:
                return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx&gid={gid}"
            return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
        except Exception:
            return url
    return url

def load_excel_from_uploaded_or_url(uploaded_file, excel_url):
    """
    Возвращает pandas.DataFrame загруженной таблицы.
    Приоритет: uploaded_file (werkzeug FileStorage) -> excel_url.
    """
    if uploaded_file:
        data = uploaded_file.read()
        return pd.read_excel(BytesIO(data), engine="openpyxl")
    if excel_url:
        url = fix_gsheet_url(excel_url)
        resp = requests.get(url)
        resp.raise_for_status()
        return pd.read_excel(BytesIO(resp.content), engine="openpyxl")
    raise FileNotFoundError("Excel файл не загружен и ссылка не указана.")

def load_json_from_filestorage(fs):
    if not fs:
        return {}
    content = fs.read()
    try:
        return json.loads(content.decode("utf-8"))
    except Exception:
        # попытаемся прочитать как байты
        return json.loads(content)

def normalize_phrase(s):
    if s is None:
        return ""
    return str(s).strip().lower()

def extract_name_phrase_map(json_data):
    """
    Ожидается стандартная структура с ключом 'intents' -> dict.
    Возвращает map: name -> {'phrases': set(...), 'priority': ... , 'types_map': {...}, 'wrong_types': [...]}
    Для гибкости: если 'intents' - список объектов, тоже обрабатываем.
    """
    result = {}
    if not json_data:
        return result
    intents = json_data.get("intents", None)
    if intents is None:
        # возможно, формат: список intents
        if isinstance(json_data, list):
            intents_list = json_data
        else:
            # попытаемся найти ключ со словарём внутри
            intents_list = []
            for v in json_data.values():
                if isinstance(v, dict) and v.get("samples"):
                    intents_list.append(v)
        for item in intents_list:
            name = item.get("name") or item.get("id") or item.get("title")
            if not name:
                continue
            phrases = set()
            types_map = {}
            for s in item.get("samples", []):
                if isinstance(s, dict):
                    text = s.get("text", "")
                    t = s.get("type", "unknown")
                else:
                    text = str(s)
                    t = "example"
                if text:
                    phrases.add(text.strip().lower())
                    types_map[text.strip().lower()] = t
            result[name] = {
                "phrases": phrases,
                "priority": item.get("priority"),
                "types_map": types_map,
                "wrong_types": []
            }
        return result

    # если intents — dict
    if isinstance(intents, dict):
        for key, item in intents.items():
            if not isinstance(item, dict):
                continue
            name = item.get("name") or key
            samples = item.get("samples", []) or []
            phrases = set()
            types_map = {}
            wrong_types = []
            for s in samples:
                if not isinstance(s, dict):
                    continue
                text = s.get("text", "").strip()
                detected_type = s.get("type", "unknown")
                if not text:
                    continue
                # определяем expected_type (упрощённо)
                if '|' in text or '?' in text:
                    expected_type = 'regex'
                elif '*' in text:
                    expected_type = 'template'
                else:
                    expected_type = 'example'
                if detected_type != expected_type:
                    wrong_types.append((text, detected_type, expected_type))
                phrases.add(text.lower())
                types_map[text.lower()] = detected_type
            result[name] = {
                "phrases": phrases,
                "priority": item.get("priority"),
                "types_map": types_map,
                "wrong_types": wrong_types
            }
        return result

    # если intents — список
    if isinstance(intents, list):
        for item in intents:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            samples = item.get("samples", [])
            phrases = set()
            types_map = {}
            for s in samples:
                if isinstance(s, dict) and s.get("text"):
                    phrases.add(s.get("text").lower())
                    types_map[s.get("text").lower()] = s.get("type", "unknown")
            result[name] = {"phrases": phrases, "priority": item.get("priority"), "types_map": types_map, "wrong_types": []}
    return result

# -------------------- Comparison functions --------------------

def compare_table_with_jsons(table_df: pd.DataFrame, json_maps: dict, json_names_map: dict):
    rows = []
    # ensure columns present
    table_df = table_df.copy()
    # normalise column names
    if "Название" not in table_df.columns:
        # try variants
        candidates = [c for c in table_df.columns if str(c).strip().lower() in ("название", "name")]
        if candidates:
            table_df = table_df.rename(columns={candidates[0]: "Название"})
    if "Приоритет" not in table_df.columns:
        candidates = [c for c in table_df.columns if str(c).strip().lower() in ("приоритет", "priority")]
        if candidates:
            table_df = table_df.rename(columns={candidates[0]: "Приоритет"})
    if "Вид" not in table_df.columns:
        candidates = [c for c in table_df.columns if str(c).strip().lower() in ("вид", "type")]
        if candidates:
            table_df = table_df.rename(columns={candidates[0]: "Вид"})
    # iterate rows
    for idx, row in table_df.iterrows():
        name = row.get("Название")
        table_priority = row.get("Приоритет")
        kind = row.get("Вид", "")
        ru_text = row.get("RU", "")
        kz_text = row.get("KZ", "")
        for key, jm in json_maps.items():
            json_filename = json_names_map.get(key, key)
            item = jm.get(name)
            if item is None:
                comment = "Название не найдено в JSON"
                json_priority = None
            else:
                json_priority = item.get("priority")
                parts = []
                if table_priority == json_priority:
                    parts.append("Приоритет совпадает")
                else:
                    parts.append(f"Приоритет: табл={table_priority} != json={json_priority}")
                normalized_samples = item.get("phrases", set())
                ru_in_samples = normalize_phrase(ru_text) in normalized_samples if (ru_text and not pd.isna(ru_text)) else False
                kz_in_samples = normalize_phrase(kz_text) in normalized_samples if (kz_text and not pd.isna(kz_text)) else False
                # if ru/kz language presence relevant, add notes
                if "ru" in key.lower() and ru_text:
                    parts.append("RU найдено" if ru_in_samples else "RU отсутствует")
                if "kz" in key.lower() and kz_text:
                    parts.append("KZ найдено" if kz_in_samples else "KZ отсутствует")
                comment = "; ".join(parts)
            equal_flag = (item is not None and table_priority == (item.get("priority") if item else None))
            rows.append({
                "Название": name,
                "Приоритет (таблица)": table_priority,
                "Приоритет (json)": json_priority,
                "Совпадает (priority)": "✅" if equal_flag else "❌",
                "JSON-файл": json_filename,
                "Вид (табл)": kind,
                "RU (табл)": ru_text,
                "KZ (табл)": kz_text,
                "Комментарий": comment
            })
    return pd.DataFrame(rows)

def compare_two_jsons(map1, map2, filename1="f1", filename2="f2", filter_names=None):
    names = set(map1.keys()) | set(map2.keys())
    if filter_names is not None:
        names = set(names) & set(filter_names)
    rows = []
    for name in sorted(names, key=lambda x: str(x)):
        i1 = map1.get(name)
        i2 = map2.get(name)
        p1 = i1.get("priority") if i1 else None
        p2 = i2.get("priority") if i2 else None
        s1 = i1.get("phrases", set()) if i1 else set()
        s2 = i2.get("phrases", set()) if i2 else set()
        added = sorted(list(s2 - s1))
        removed = sorted(list(s1 - s2))
        same = (s1 == s2 and p1 == p2)
        comment_parts = []
        if p1 != p2:
            comment_parts.append(f"Приоритет: {p1} -> {p2}")
        if added:
            comment_parts.append(f"Добавлено {len(added)}")
        if removed:
            comment_parts.append(f"Удалено {len(removed)}")
        if not i1:
            comment_parts.append("Присутствует только в " + filename2)
        if not i2:
            comment_parts.append("Присутствует только в " + filename1)
        comment = "; ".join(comment_parts) if comment_parts else "Совпадает"
        rows.append({
            "Название": name,
            f"Приоритет ({filename1})": p1,
            f"Приоритет ({filename2})": p2,
            f"Кол-во фраз ({filename1})": len(s1),
            f"Кол-во фраз ({filename2})": len(s2),
            "Добавлено (первые 10)": ", ".join(added[:10]) + ("..." if len(added) > 10 else ""),
            "Удалено (первые 10)": ", ".join(removed[:10]) + ("..." if len(removed) > 10 else ""),
            "Совпадает полностью": "✅" if same else "❌",
            "Файл1": filename1,
            "Файл2": filename2,
            "Комментарий": comment
        })
    return pd.DataFrame(rows)

# -------------------- Flask routes --------------------

@app.route("/", methods=["GET", "POST"])
def index():
    results_html = ""
    error = None

    if request.method == "POST":
        action = request.form.get("action")
        if action == "clear":
            return redirect(url_for("index"))

        # загрузим JSONs (файлы могут быть пустыми)
        try:
            js1 = load_json_from_filestorage(request.files.get("json1"))
            js2 = load_json_from_filestorage(request.files.get("json2"))
            js3 = load_json_from_filestorage(request.files.get("json3"))
            js4 = load_json_from_filestorage(request.files.get("json4"))
        except Exception as e:
            error = f"Ошибка при чтении JSON файлов: {e}"
            return render_template_string(HTML, results_html=None, error=error)

        try:
            table_df = None
            try:
                table_df = load_excel_from_uploaded_or_url(request.files.get("excel_file"), request.form.get("excel_url", "").strip())
            except Exception as e:
                error = f"Ошибка при загрузке Excel: {e}"
                return render_template_string(HTML, results_html=None, error=error)
        except Exception as e:
            error = f"Ошибка при подготовке таблицы: {e}"
            return render_template_string(HTML, results_html=None, error=error)

        # извлечём структуры
        map1 = extract_name_phrase_map(js1) if js1 else {}
        map2 = extract_name_phrase_map(js2) if js2 else {}
        map3 = extract_name_phrase_map(js3) if js3 else {}
        map4 = extract_name_phrase_map(js4) if js4 else {}

        json_maps = {}
        name_map = {}
        if map1:
            json_maps["JSON1"] = map1; name_map["JSON1"] = request.files.get("json1").filename or "JSON1"
        if map2:
            json_maps["JSON2"] = map2; name_map["JSON2"] = request.files.get("json2").filename or "JSON2"
        if map3:
            json_maps["JSON3"] = map3; name_map["JSON3"] = request.files.get("json3").filename or "JSON3"
        if map4:
            json_maps["JSON4"] = map4; name_map["JSON4"] = request.files.get("json4").filename or "JSON4"

        # Общая таблица Table_vs_JSONs
        table_vs = compare_table_with_jsons(table_df, json_maps, name_map)

        # в зависимости от action - выполняем логику
        # 2: compare 1 vs 2, and both vs table
        # 4: compare 1 vs 2, 3 vs 4, 1 vs 3 only for names with Вид == "Общий", 4 vs 2 same condition
        try:
            with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
                table_vs.to_excel(writer, sheet_name="Table_vs_JSONs", index=False)

            if action == "compare_2":
                # need map1 & map2
                if not map1 or not map2:
                    error = "Для сравнения 2 файлов нужны JSON1 и JSON2 загруженные."
                    writer.close()
                    return render_template_string(HTML, results_html=None, error=error)
                df_12 = compare_two_jsons(map1, map2, name_map.get("JSON1", "JSON1"), name_map.get("JSON2", "JSON2"))
                df_12.to_excel(writer, sheet_name="JSON1_vs_JSON2", index=False)
                # already table_vs sheet saved
                # финальные таблицы - подготавливаем результат HTML
                results_html += "<h4>JSON1 vs JSON2</h4>" + df_12.to_html(index=False, escape=False)
            elif action == "compare_4":
                # compare 1 vs 2
                if map1 and map2:
                    df_12 = compare_two_jsons(map1, map2, name_map.get("JSON1", "JSON1"), name_map.get("JSON2", "JSON2"))
                    df_12.to_excel(writer, sheet_name="JSON1_vs_JSON2", index=False)
                    results_html += "<h4>JSON1 vs JSON2</h4>" + df_12.to_html(index=False, escape=False)
                else:
                    results_html += "<p>JSON1 или JSON2 отсутствует — пропускаем сравнение 1 vs 2.</p>"

                # compare 3 vs 4
                if map3 and map4:
                    df_34 = compare_two_jsons(map3, map4, name_map.get("JSON3", "JSON3"), name_map.get("JSON4", "JSON4"))
                    df_34.to_excel(writer, sheet_name="JSON3_vs_JSON4", index=False)
                    results_html += "<h4>JSON3 vs JSON4</h4>" + df_34.to_html(index=False, escape=False)
                else:
                    results_html += "<p>JSON3 или JSON4 отсутствует — пропускаем сравнение 3 vs 4.</p>"

                # cross compares only for names where table 'Вид' == 'Общий'
                common_names = set()
                if "Вид" in table_df.columns:
                    try:
                        common_names = set(table_df.loc[table_df["Вид"].astype(str).str.strip().str.lower() == "общий", "Название"].tolist())
                    except Exception:
                        common_names = set()
                if not common_names:
                    results_html += "<p>Нет строк с 'Вид' == 'Общий' — перекрёстные сравнения пропущены.</p>"

                # 1 vs 3 for common_names
                if map1 and map3 and common_names:
                    df_13 = compare_two_jsons(map1, map3, name_map.get("JSON1","JSON1"), name_map.get("JSON3","JSON3"), filter_names=common_names)
                    df_13.to_excel(writer, sheet_name="JSON1_vs_JSON3_common", index=False)
                    results_html += "<h4>JSON1 vs JSON3 (Вид=='Общий')</h4>" + df_13.to_html(index=False, escape=False)
                else:
                    results_html += "<p>JSON1 или JSON3 отсутствует или нет общих имён — пропускаем 1 vs 3.</p>"

                # 4 vs 2 for common_names
                if map4 and map2 and common_names:
                    df_42 = compare_two_jsons(map4, map2, name_map.get("JSON4","JSON4"), name_map.get("JSON2","JSON2"), filter_names=common_names)
                    df_42.to_excel(writer, sheet_name="JSON4_vs_JSON2_common", index=False)
                    results_html += "<h4>JSON4 vs JSON2 (Вид=='Общий')</h4>" + df_42.to_html(index=False, escape=False)
                else:
                    results_html += "<p>JSON4 или JSON2 отсутствует или нет общих имён — пропускаем 4 vs 2.</p>"

            # всегда добавляем Table_vs_JSONs (мы уже записали)
            # записываем общий лист со всеми json (если нужны)
            # ещё запишем сводный лист с перечислением доступных json файлов
            meta_df = pd.DataFrame([{"Loaded JSON": v} for v in name_map.values()])
            meta_df.to_excel(writer, sheet_name="Loaded_JSONs", index=False)

            with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
                table_vs.to_excel(writer, sheet_name="Table_vs_JSONs", index=False)

            # добавим table_vs html
            results_html = "<h4>Table vs JSONs</h4>" + table_vs.to_html(index=False, escape=False) + results_html

            return render_template_string(HTML, results_html=results_html, error=None)

        except Exception as e:
            error = f"Ошибка во время сравнения/записи Excel: {e}"
            return render_template_string(HTML, results_html=None, error=error)

    return render_template_string(HTML, results_html=None, error=None)

@app.route("/download")
def download():
    if not OUTPUT_XLSX.exists():
        return "Файл результата не найден. Сначала выполните сравнение."
    return send_file(OUTPUT_XLSX, as_attachment=True)

if __name__ == "__main__":
    # запустить локально
    app.run(debug=True)
