"""
Génère frontend/public/communes-map.json depuis la SQLite locale.
A relancer après chaque import de données.
Lancer depuis /Users/admin/vivreici/ :
  python scripts/export_map_json.py
"""
import sqlite3
import json
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'vivreici.db')
OUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'public', 'communes-map.json')

PLM_PARENTS = {'75056', '69123', '13055'}

conn = sqlite3.connect(DB_PATH)
cursor = conn.execute("""
    SELECT c.code_insee, c.nom, c.population, c.latitude, c.longitude,
           s.score_global, s.lettre
    FROM communes c
    JOIN scores s ON c.code_insee = s.code_insee
    WHERE c.latitude IS NOT NULL
      AND c.longitude IS NOT NULL
      AND s.nb_categories_scorees >= 3
    ORDER BY c.population DESC
""")

communes = []
for row in cursor.fetchall():
    code = row[0]
    if code in PLM_PARENTS:
        continue
    lettre = row[6] if row[6] in ('A', 'B', 'C', 'D', 'E') else None
    communes.append({
        "code_insee": code,
        "nom": row[1],
        "population": row[2],
        "latitude": round(row[3], 5),
        "longitude": round(row[4], 5),
        "score_global": round(row[5], 1) if row[5] is not None else None,
        "lettre": lettre,
    })

conn.close()

with open(OUT_PATH, 'w', encoding='utf-8') as f:
    json.dump(communes, f, separators=(',', ':'), ensure_ascii=False)

size_kb = os.path.getsize(OUT_PATH) / 1024
print(f"Exported {len(communes)} communes → {OUT_PATH} ({size_kb:.0f} KB)")
