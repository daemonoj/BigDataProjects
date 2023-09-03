E = LOAD '$G' using PigStorage(',') as (user: int, rating: int);
F = GROUP E by user;
total_ratings = FOREACH F generate group, (int)((double)SUM(E.$1)/(double)COUNT(E) * 10);
C = GROUP total_ratings by $1;
result = FOREACH C generate (double)group/10.0, COUNT(total_ratings);
result = FILTER result BY NOT $1 == 0;
STORE result INTO '$O' using PigStorage('\t');


