E = LOAD 'e.txt' USING PigStorage(',') AS (ename, dno, address);
D = LOAD 'd.txt' USING PigStorage(',') AS (dname, dno);
J = JOIN E BY dno, D BY dno;
O = FOREACH J GENERATE ename, dname, address;
STORE O INTO 'output' USING PigStorage (',');
