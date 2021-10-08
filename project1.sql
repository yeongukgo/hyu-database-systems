SELECT name
FROM Pokemon
WHERE type = 'Grass'
ORDER BY name;

SELECT name
FROM Trainer
WHERE hometown = 'Brown City'
  OR hometown = 'Rainbow City'
ORDER BY name;

SELECT DISTINCT type
FROM Pokemon
ORDER BY type;

SELECT name
FROM City
WHERE name LIKE 'B%'
ORDER BY name;

SELECT hometown
FROM Trainer
WHERE name NOT LIKE 'M%'
ORDER BY hometown;

SELECT nickname
FROM CatchedPokemon
WHERE level = (SELECT MAX(level)
               FROM CatchedPokemon)
ORDER BY nickname;

SELECT name
FROM Pokemon
WHERE name LIKE 'A%'
  OR name LIKE 'E%'
  OR name LIKE 'I%'
  OR name LIKE 'O%'
  OR name LIKE 'U%'
ORDER BY name;

SELECT AVG(level)
FROM CatchedPokemon;

SELECT MAX(cp.level)
FROM CatchedPokemon cp
JOIN Trainer t ON cp.owner_id = t.id
  AND t.name = 'Yellow';

SELECT DISTINCT hometown
FROM Trainer
ORDER BY hometown;

SELECT t.name, cp.nickname
FROM Trainer t
JOIN CatchedPokemon cp ON t.id = cp.owner_id
  AND cp.nickname LIKE 'A%'
ORDER BY t.name;

SELECT t.name
FROM Trainer t
JOIN Gym g ON t.id = g.leader_id
JOIN City c ON g.city = c.name
  AND c.description = 'Amazon';

SELECT cp.owner_id, COUNT(cp.owner_id) AS cnt
FROM CatchedPokemon cp
JOIN Pokemon p ON cp.pid = p.id
  AND p.type = 'Fire'
GROUP BY cp.owner_id
HAVING cnt = (SELECT MAX(cnt)
              FROM (SELECT COUNT(cp.owner_id) AS cnt
                    FROM CatchedPokemon cp
                    JOIN Pokemon p ON cp.pid = p.id
                      AND p.type = 'Fire'
                    GROUP BY cp.owner_id) sub);

SELECT DISTINCT type
FROM Pokemon
WHERE id < 10
ORDER BY id DESC;

SELECT COUNT(*)
FROM Pokemon
WHERE NOT type = 'FIRE';

SELECT p.name
FROM Pokemon p
JOIN Evolution ev ON p.id = ev.before_id
  AND ev.before_id > ev.after_id
ORDER BY p.name;

SELECT AVG(cp.level)
FROM CatchedPokemon cp
JOIN Pokemon p ON cp.pid = p.id
  AND p.type = 'Water';

SELECT cp.nickname
FROM CatchedPokemon cp
JOIN Gym g ON cp.owner_id = g.leader_id
WHERE cp.level = (SELECT MAX(cp.level)
                  FROM CatchedPokemon cp, Gym g
                  WHERE cp.owner_id = g.leader_id);

SELECT t.name
FROM Trainer t
JOIN CatchedPokemon cp ON t.id = cp.owner_id
WHERE t.hometown = 'Blue City'
GROUP BY t.name
HAVING AVG(level) = (SELECT MAX(avglev)
                     FROM (SELECT AVG(level) AS avglev
                           FROM Trainer t
                           JOIN CatchedPokemon cp ON t.id = cp.owner_id
                           WHERE t.hometown = 'Blue City'
                           GROUP BY t.name) sub)
ORDER BY t.name;

SELECT p.name
FROM Pokemon p
JOIN CatchedPokemon cp ON p.id = cp.pid
WHERE p. id IN (SELECT before_id
                FROM Evolution)
  AND p.type = 'Electric'
  AND cp.owner_id IN (SELECT id
                      FROM Trainer
                      GROUP BY hometown
                      HAVING COUNT(*) = 1);

SELECT t.name, SUM(cp.level) AS levelsum
FROM Trainer t
JOIN Gym g ON t.id = g.leader_id
JOIN CatchedPokemon cp ON g.leader_id = cp.owner_id
GROUP BY t.name
ORDER BY levelsum DESC;

SELECT hometown
FROM Trainer
GROUP BY hometown
HAVING COUNT(*) = (SELECT MAX(cnt)
                   FROM (SELECT hometown, COUNT(*) AS cnt
                         FROM Trainer
                         GROUP BY hometown) sub);

SELECT DISTINCT p.name
FROM Pokemon p
JOIN (SELECT cp.pid, COUNT(DISTINCT t.hometown) As cnt
      FROM CatchedPokemon cp
      JOIN Trainer t ON cp.owner_id = t.id
        AND (t.hometown = 'Sangnok City'
             OR t.hometown = 'Brown City')
      GROUP BY cp.pid) sub ON p.id = sub.pid
  AND sub.cnt = 2
ORDER BY p.name;

SELECT DISTINCT t.name
FROM Trainer t
JOIN CatchedPokemon cp ON t.id = cp.owner_id
JOIN Pokemon p ON cp.pid = p.id
  AND p.name LIKE 'P%'
WHERE t.hometown = 'Sangnok City'
ORDER BY t.name;

SELECT  t.name, p.name
FROM Trainer t
JOIN CatchedPokemon cp ON t.id = cp.owner_id
JOIN Pokemon p ON cp.pid = p.id
ORDER BY t.name, p.name;

SELECT p.name
FROM Pokemon p
JOIN Evolution ev ON p.id = ev.before_id
  AND ev.before_id NOT IN (SELECT after_id
                           FROM Evolution)
  AND ev.after_id NOT IN (SELECT before_id
                          FROM Evolution)
ORDER BY p.name;

SELECT cp.nickname
FROM CatchedPokemon cp
JOIN Gym g ON cp.owner_id = g.leader_id
  AND g.city = 'Sangnok City'
JOIN Pokemon p ON cp.pid = p.id
  AND p.type = 'Water'
ORDER BY nickname;

SELECT name
FROM Trainer
WHERE id IN (SELECT owner_id
             FROM CatchedPokemon
             WHERE pid IN (SELECT after_id
                           FROM Evolution)
             GROUP BY owner_id
             HAVING COUNT(*) >= 3)
ORDER BY name;

SELECT name
FROM Pokemon
WHERE id NOT IN (SELECT DISTINCT pid
                 FROM CatchedPokemon)
ORDER BY name;

SELECT MAX(cp.level) AS maxlev
FROM CatchedPokemon cp
JOIN Trainer t ON cp.owner_id = t.id
GROUP BY t.hometown
ORDER BY maxlev DESC;

SELECT p1.id, p1.name, p2.name, p3.name
FROM (SELECT ev1.before_id AS id1, ev1.after_id AS id2, ev2.after_id AS id3
      FROM Evolution ev1, Evolution ev2
      WHERE ev1.after_id = ev2.before_id) sub
JOIN Pokemon p1 ON sub.id1 = p1.id
JOIN Pokemon p2 ON sub.id2 = p2.id
JOIN Pokemon p3 ON sub.id3 = p3.id
ORDER BY p1.id;