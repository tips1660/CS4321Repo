--SELECT * FROM Sailors;
/*
SELECT DISTINCT ON( R.G) R.G FROM Reserves R;
SELECT DISTINCT * FROM Sailors;
*/

/* Testing block */
SELECT X.F FROM Boats AS X Order By X.F
SELECT * FROM Sailors ORDER BY Sailors.B, Sailors.A;
SELECT R.G, S.A FROM Sailors S, Reserves R;
Select X.A, P.G From Sailors X, Reserves P
Select X.A, P.G, D.D From Sailors X, Reserves P, Boats D
SELECT S.A, S.B FROM Sailors S Where S.A >2 AND S.B>100
Select R.G FROM Reserves R
Select X.F From Boats X
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
Select * From Sailors S, Reserves R
Select * From Sailors X, Reserves P
Select S.A, P.G From Sailors S, Reserves P
SELECT * FROM Sailors S, Reserves R  WHERE S.A = R.G AND S.A >= 4
Select * from Sailors S, Boats B, Reserves R Where S.A>2 AND B.D=R.H and B.E = S.A and R.G >1
Select * from Boats, Sailors, Reserves Where Sailors.A>2 AND Reserves.H=Boats.D and Boats.E = Sailors.A and Reserves.G >1 
SELECT Sailors.A FROM Sailors WHERE 3 < Sailors.A
SELECT Sailors.A FROM Sailors WHERE 3 < Sailors.A AND Sailors.C > 100
SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C AND Sailors.A>=1 
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G AND Sailors.A >= 4
SELECT Sailors.A FROM Sailors;
SELECT Sailors.B FROM Sailors;
SELECT Sailors.A, Sailors.C FROM Sailors;
SELECT Reserves.G FROM Reserves;
SELECT * FROM Reserves;
SELECT Boats.E FROM Boats;
Select * From Sailors, Boats, Reserves 
Select * from Sailors, Boats 
Select * From Sailors, Reserves 
Select * From Reserves, Sailors
Select * From Boats, Sailors
Select * From Reserves, Boats
Select * From Boats, Reserves order by Boats.D, Boats.E
SELECT * FROM Sailors, Boats, Reserves WHERE Sailors.A>2 AND  Boats.D=Reserves.H and Boats.E = Sailors.A
Select * From Reserves, Boats, Sailors WHERE Sailors.A >2 AND Boats.D = Reserves.H and Sailors.A = Boats.E 
Select * from Reserves, Boats, Sailors Where Sailors.A=3 AND Reserves.H=Boats.D and Boats.E = Sailors.A and Reserves.G >1 
Select * from Sailors, Boats, Reserves Where Sailors.A>2 AND Boats.D=Reserves.H and Boats.E = Sailors.A and Reserves.G >1 
Select * from Boats, Sailors, Reserves Where Sailors.A>2 AND Boats.D=Reserves.H and Boats.E = Sailors.A and Reserves.G >1








