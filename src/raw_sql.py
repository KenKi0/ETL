""" Raw SQL queries."""

# Можно ли оптимизировать запросы?
# Например как то объеденить запросы person_film_id и person_films

film = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating as imdb_rating,
   fw.created_at,
   fw.updated_at,
   COALESCE(ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
   FILTER (WHERE pfw.role = 'director' AND p.id is not null), '{}') AS director,
   ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
   FILTER (WHERE pfw.role = 'actor' AND p.id is not null) AS actors,
   ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
   FILTER (WHERE pfw.role = 'writer' AND p.id is not null) AS writers,
   ARRAY_AGG(DISTINCT p.full_name) 
   FILTER (WHERE pfw.role = 'actor' AND p.id is not null) AS actors_names,
   ARRAY_AGG(DISTINCT p.full_name) 
   FILTER (WHERE pfw.role = 'writer' AND p.id is not null) AS writers_names,
   ARRAY_AGG(DISTINCT g.name)
   FILTER (WHERE g.id is not null) as genre
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.updated_at > %s
GROUP BY fw.id
ORDER BY fw.updated_at;
"""

person_id = """
SELECT id, updated_at
FROM content.person
WHERE updated_at > %s
ORDER BY updated_at;
"""

persons = """
SELECT p.id, 
       p.full_name,
       p.updated_at,
       ARRAY_AGG(DISTINCT pfw.role) as role,
       ARRAY_AGG(DISTINCT pfw.film_work_id)::text[] as film_ids
FROM content.person p
LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
WHERE updated_at > %s
GROUP BY p.id, role
ORDER BY updated_at;
"""

person_film_id = """
SELECT fw.id
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
WHERE pfw.person_id IN %s AND p.updated_at > fw.updated_at AND fw.updated_at > %s
ORDER BY fw.updated_at;
"""

person_films = """
SELECT DISTINCT fw.id as film_id, 
       fw.updated_at,
       COALESCE(ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
       FILTER (WHERE pfw.role = 'director' AND p.id is not null), '{}') AS director,
       ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
       FILTER (WHERE pfw.role = 'actor' AND p.id is not null) AS actors,
       ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
       FILTER (WHERE pfw.role = 'writer' AND p.id is not null) AS writers,
       ARRAY_AGG(DISTINCT p.full_name) 
       FILTER (WHERE pfw.role = 'actor' AND p.id is not null) AS actors_names,
       ARRAY_AGG(DISTINCT p.full_name) 
       FILTER (WHERE pfw.role = 'writer' AND p.id is not null) AS writers_names
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
WHERE fw.id IN %s
GROUP BY fw.id
ORDER BY fw.updated_at;
"""

genre_id = """
SELECT id, updated_at
FROM content.genre
WHERE updated_at > %s
ORDER BY updated_at;
"""

genre_film_id = """
SELECT fw.id, fw.updated_at
FROM content.film_work fw
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE gfw.genre_id IN %s AND g.updated_at > fw.updated_at AND fw.updated_at > %s
ORDER BY fw.updated_at;
"""


genre_films = """
SELECT DISTINCT fw.id as film_id, 
       fw.updated_at,
       ARRAY_AGG(DISTINCT g.name)
       FILTER (WHERE g.id is not null) as genre
FROM content.film_work fw
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id IN %s
GROUP BY fw.id
ORDER BY fw.updated_at;
"""
