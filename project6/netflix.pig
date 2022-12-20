L = LOAD '$G' USING PigStorage(',') AS (p: int, q: double);
Q = FILTER L BY (p IS NOT NULL);
G = GROUP Q BY p;

UserRating = FOREACH G GENERATE group, AVG(Q.q) as S;
B = GROUP UserRating BY FLOOR(S*10)/10;
Stats = FOREACH B GENERATE group, COUNT(UserRating);

STORE Stats INTO '$O' USING PigStorage (' ');