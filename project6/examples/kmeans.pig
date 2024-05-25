P = LOAD $P USING PigStorage(',') AS ( x:double, y:double );
C = LOAD $C USING PigStorage(',') AS ( x:double, y:double );
J = FOREACH P {
       N = FOREACH C GENERATE C.x+P.x, C.x, C.y AS (distance,x,y);
       GENERATE P.x, P.Y, N;
};
STORE O INTO 'output' USING PigStorage (',');
