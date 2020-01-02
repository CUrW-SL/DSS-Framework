(* ::Package:: *)


(* ::Section::Initialization:: *)
(*(*Functions*)*)


(* ::Input::Initialization:: *)
Needs["DatabaseLink`"];


(* ::Input::Initialization:: *)
JDBCDrivers["MySQL(Connector/J)"];


(* ::Input::Initialization:: *)
getTimeFromFn[fn_]:= Module[{str},
str =StringSplit[StringDrop[Last[StringSplit[fn,"/"]],-4],"_"];
str = StringSplit[str[[4]]<>"-"<>str[[5]],"-"];
DateObject[str[[1]]<>"-"<>str[[2]]<>"-"<>str[[3]]<>" "<>str[[4]]<>":"<>str[[5]]<>":00",TimeZone -> 5.5]]


(* ::Input::Initialization:: *)
prDt[dt_]:= DateString[dt,{"Year","-","Month","-","Day"," ","Hour",":","Minute",":00"}]


(* ::Input::Initialization:: *)
rotateLable[fn_]:= Rotate[DateString[getTimeFromFn[fn],{"Month","Day"," ","Hour"}],90]


(* ::Input::Initialization:: *)
fc[x_] := Module[{cc},
   If[x <= 0, cc = Transparent];
If[x > 0  && x <= 2, cc = LightGreen];
   If[x > 2  && x <= 5, cc = LightBlue ];
 If[x > 5 && x <= 10, cc = Lighter[Cyan]];
 If[x > 10 && x <= 15, cc = Cyan];
   If[x > 15 && x <= 30, cc = Blue];
   If[x > 30 && x <= 50, cc = Orange];
   If[x > 50 && x <= 80, cc = Red];
   If[x > 80, cc = Purple];
   cc
  ]


(* ::Input::Initialization:: *)
fcc[x_] := Module[{cc},
If[x<= 0,Transparent];
If[x > 0 &&x <= 5, cc = Green];
   If[x > 5 &&x <= 10, cc = Green];
   If[x > 10  && x <= 30, cc = LightBlue];
   If[x > 30 && x <= 50, cc = Cyan];
   If[x > 50 && x <= 100, cc = Blue];
   If[x > 100 && x <= 150, cc = Red];
 If[x > 150 && x <= 200, cc = Brown];
   If[x > 200, cc = Purple];
   cc
  ]


(* ::Input::Initialization:: *)
readDirModelAdj[dir_,rgn_,selMod_,mf_,st_]:= Module[{ver,wrf,data,model,version,datetime,rdata,dd,list,gridlist,prim,g,grp,gr,grid,gridrow, dates, versions, models,n,
minlat,maxlat,minlon,maxlon,i,rplot,rcplot,xy,dtot,kst,max,m},
ver = {"4.0"};
 wrf = <||>;
max = 0.;
{{minlon,maxlon},{minlat,maxlat}}= rgn;
list =StringSplit[dir,"/"];
xy = Import[dir<>"/"<>selMod<>"/xy.csv","csv"];
Print["Read xy Size ", Length[xy], " " , Length[First[xy]]];
data = FileNames[dir<>"/" <>selMod<>"/WRF*"];
Print[Length[data]];
Print[data[[1]]];
Do[
(*list = StringCases[data[[i]],
 RegularExpression["WRF_"<>selMod<>"_4.0_(.* )_(.* ).txt"] -> {"$1", "$2"}];
If[Length[list] != 0,
datetime = list[[1]][[1]] <> " " <> list[[1]][[2]];
n=DateObject[{datetime,{"Year","-","Month","-","Day"," ","Hour","-","Minute"}},TimeZone \[Rule] 5.5 ];
If[n != getTimeFromFn[data[[i]]], Print["Error in Time Parsing"];
Abort[]];*)
n = getTimeFromFn[data[[i]]];
If[n>st,
datetime = DateString[n];
rdata =ReadList[data[[i]], Number];
If[! (KeyExistsQ[wrf, datetime]), AppendTo[wrf, datetime -> <||>]];wrf[datetime]= rdata],{i,1,Length[data]}];
(*Print[datetime,Length[wrf[datetime]]];*)
gridlist = {};
dates = Keys[wrf];
(*Print["dates ",Length[dates]];*)
grid = {};
Do[
gridrow = {};
dd = wrf[k] * mf;
If[k == dates[[1]],
kst = k;
dtot = dd,
dtot = dtot+dd];
If [m=Max[dtot] > max, max=m];
(*AppendTo[grid,Plus@@ dtot];*)
rplot =
ListDensityPlot[Transpose[{xy[[All,1]],xy[[All,2]],4. dd}],ColorFunction -> (fc[#] &), PlotRange -> {{minlon,maxlon}, {minlat,maxlat},All},PlotLegends -> None, ColorFunctionScaling -> False];prim = First@Cases[rplot, Graphics[a_, ___] :> a, {0, -1}, 1];gr = GeoGraphics[{Opacity[0.4], prim, GeoRangePadding -> None},(*GeoServer \[Rule] "http://a/tile.openstreetmap.org/`1`/`2`/`3`.png",*)ImageSize -> Large];grp = Labeled[gr,("WRF " <> selMod<>" @ "<>k),LabelStyle->Directive[Blue,Large]];AppendTo[gridrow, grp];
rcplot = ListDensityPlot[Transpose[{  xy[[All,1]],xy[[All,2]],dtot}],ColorFunction -> (fcc[#] &), PlotRange -> {{minlon,maxlon}, {minlat,maxlat},All},PlotLegends -> None, ColorFunctionScaling -> False];
prim = First@Cases[rcplot, Graphics[a_, ___] :> a, {0, -1}, 1];gr = GeoGraphics[{Opacity[0.4], prim, GeoRangePadding -> None},(*GeoServer \[Rule] "http://a/tile.openstreetmap.org/`1`/`2`/`3`.png",*)ImageSize -> Large];
grp = Labeled[gr,("Total Rain " <> kst<>" - "<>k),LabelStyle->Directive[Blue,Large]];
AppendTo[gridrow, grp];
AppendTo[gridrow, BarLegend[{fc[#] &, {0, 100}}, {2,5, 10, 15,30, 50, 80},LegendLayout -> "Row",LabelStyle->Directive[Gray,FontSize->14,Bold],LegendLabel->"Rain mm/hr"]];
AppendTo[gridrow,  BarLegend[{fcc[#] &, {0, 250}}, {0,5,10, 30, 50, 100,150,200},LegendLayout -> "Row",LabelStyle->Directive[Gray,FontSize->14,Bold],LegendLabel->"Total R(mm) from "<>kst]];AppendTo[gridlist,Grid[Partition[gridrow,2], Frame -> All, Spacings -> {0, 0}]]
  , {k, dates}];
Print["Max Cumulative Rain ",m];
 Export[dir <> "WRFgrid.gif", gridlist, ImageSize->1500,"DisplayDurations" -> 0.2,"AnimationRepetitions"->Infinity ,"ControlAppearance"->None]
(*grid*)
 ]


(* ::Input::Initialization:: *)
makeWrfTotVoronoi[dir_,extent_,model_]:= Module[{fn, dd,dtot,nc,pts={},list,rplot,gr,grid,ticks={},lab},
SetDirectory[dir];
fn = FileNames["/"<>model<>"/WRF*"];
(*Print[Length[fn]];*)
Do[
dd=Map[ToExpression,ReadList[fn[[i]], Word,WordSeparators->{" "},RecordLists -> True], 2];
nc=Length[dd];
If[i==1,
dtot = dd,
dtot = dtot + dd];
AppendTo[pts, {i,First[(Plus@@dtot) / nc]}]
,{i, 1, Length[fn]}];
{pts, fn}]


(* ::Input::Initialization:: *)
makeWrfTotInRegion[dir_,rgnpts_,model_]:= Module[{fn, dd,dtot,nc,pts={},list,rplot,gr,grid,ticks={},lab},
SetDirectory[dir<>"/"<>model];
fn = FileNames["WRF_*"];
(*Print[Length[fn]];*)
Do[
dd=Part[Map[ToExpression,ReadList[fn[[i]], Word,WordSeparators->{" "},RecordLists -> True], 2],rgnpts];
nc=Length[dd];
If[i==1,
dtot = dd,
dtot = dtot + dd];
AppendTo[pts, First[(Plus@@dtot) / nc]]
,{i, 1, Length[fn]}];
{pts, fn}]


(* ::Input::Initialization:: *)
voronoiArea[d_, {{minlon_,maxlon_},{minlat_,maxlat_}}]:=Block[{v},
v =VoronoiMesh[d[[All,{1,2}]],{{minlon,maxlon},{minlat,maxlat}}];
(*Show[v,Graphics[{Orange,Point[pts]}]]*)
Area /@ MeshPrimitives[v,2]]


(* ::Input::Initialization:: *)
makeVoronoiInRegion[dir_, extent_, models_,dt_]:= Module[
{rgnpts={},pts = {},xy, ticks,tmp,n,con,s,e,str,d,et,ts,maxlat,minlat,maxlon,minlon,obs={},obsr={},i,rs={}, areas,v,ta,key,r,list},
{{minlon,maxlon},{minlat,maxlat}}=extent ;
list=StringSplit[dir,"/"];
xy = Import[dir<>"/"<>First[models]<>"/xy.csv","csv"];
i=1;
While[i<Length[xy],
If[minlon<= xy[[i,1]]<= maxlon && minlat<= xy[[i,2]]<= maxlat,AppendTo[rgnpts,i]];
i=i+1];
tmp = makeWrfTotInRegion[dir,rgnpts,#]& /@ models;
ts = getTimeFromFn /@ First[tmp][[2]];
AppendTo[pts, First[#]]& /@ tmp;
con=OpenSQLConnection[JDBC["MySQL(Connector/J)", "35.227.163.211"],"Name" ->"curw_obs", "Username" -> "admin", "Password" -> "floody","UseConnectionPool"->True, "Properties" -> {"useSSL" ->"false"}];
Print[prDt[First[ts]]," : ",prDt[dt]," : ",prDt[Last[ts]]];
areas = <||>;
(*areas = Get[dir<>"areasAssoc"];*)
i=2;
While[ts[[i]]<dt,
str =StringJoin["call curw_obs.getCumulativeObservedRainfallGivenRegion('",prDt[ts[[i-1]]],"','",prDt[ts[[i]]],"',",ToString[minlon],",",ToString[maxlon],",",ToString[minlat],",",ToString[maxlat],")"];d=SQLExecute [con, str];
n=Length[d];
If[n > 1,
(*Print["Data Length ", n, " at ", i, "time ", ts[[i]]," till ", dt];*)
AppendTo[obsr,d ];
key={extent,d[[All,{1,2}]]};
If[!(KeyExistsQ[areas,key]),
AppendTo[areas, key -> <||>];
areas[key] = voronoiArea[d,extent];
Print["No Key  ", DateString[ts[[i]]], " Areas ", areas[key]]
];
ta = Plus @@ areas[key];
r = Plus@@(d[[All,3]] areas[key]) ;
AppendTo[rs,r/ta],
If[n==1,
AppendTo[rs,d[[1,3]]]]];
i=i+1];
CloseSQLConnection[con];
Put[areas,dir<>"areasAssoc"];
obs = FoldList[Plus,rs];
ticks = rotateLable[#]& /@ First[tmp][[2]];
ticks =First /@  Partition[Transpose[{Range[Length[ticks]],ticks}],16];
AppendTo[pts,obs];
Put[pts, dir<>"rs"<>prDt[dt]];
Export[dir<>"fcstComp.gif",ListPlot[pts, Ticks -> {ticks,Automatic}, PlotLegends->Append[models,"Obs"] ]];
(*Export[dir<>"obsSpatial.gif",Table[ListDensityPlot[obsr[[i]],InterpolationOrder-> 0, PlotRange ->{{minlon,maxlon},{minlat,maxlat},All}, PlotLabel->DateString[ts[[i]],{"Month","/","Day",":","Hour"}]],{i,1,Length[obsr]}],"DisplayDurations" -> 0.2,"AnimationRepetitions"->Infinity ,"ControlAppearance"->None]*);
Put[obsr,dir<>"obsSpatialData"<>prDt[dt]];
(*ticks*)
{ticks,obsr}
]


(* ::Input::Initialization:: *)
normaliDif[ob_,fc_]:=Module[{n},
n = Length[ob];
If[ob[[n]]>0.,
If[fc[[n]]>0.,
ob/ob[[n]] - Take[fc,{1,n}]/fc[[n]],ob[[n]]],
Take[fc,{1,n}]]]


nmd[rs_]:= Module[{n,m,rsm,x,d1,d2,mp,difs,mf},
n = Length[rs];
m = Min[Length /@ rs];
rsm = Take[#,{1,m}]& /@ rs;
x=Abs /@ (# -Last[rsm]&/@ Most[rsm]);
d1=Plus @@#& /@ x;
d1 =d1/Max[1,Max[d1]];
If[rs[[n,m]]>0.,
d2 = rsm[[All,m]]/rs[[n,m]],
d2 = rsm[[All,m]]];
d2 = d2/Max[1,Max[d2]];
difs= d1 + Most[d2];
mp = First[Flatten[Position[difs, Min[difs]]]];
mf =rs[[n,m]]/rs[[mp,m]];
mf = Min[mf, 3.];
mf = Max[0.5,mf];
 {mp, mf}
]


(* ::Input:: *)
(*Clear[dataDir]*)


(* ::Input::Initialization:: *)
dataDir[basin_, time_]:= "/home/uwcc-admin/wrf_rfields/4.1.2/d0/"<>time<>"/rfields/wrf/"<>basin


(* ::Input:: *)
(*dataDir["d03_kelani_basin","18"]*)


(* ::Input::Initialization:: *)
todayModels[basin_,time_]:={"A","C","E","SE"};


(* ::Input:: *)
(*Clear[makeWrfFcst]*)


(* ::Input::Initialization:: *)
makeWrfFcst[basin_,t_]:= makeWrfFcst[basin,t, {{79.84343675, 80.15575615},{6.774218729,7.042994907}}]


(* ::Input::Initialization:: *)
twoFcst[t_] := Module[{},
Print["starting wrf...."];
makeWrfFcst["d03_kelani_basin",t, kelaniRgn];
(*writeToCloud[dataDir["d03_kelani_basin",t]];*)
makeWrfFcst["d03",t, d03r];
(*writeToCloud[dataDir["d03",t]]*)
]

Clear[makeWrfFcst]
(* ::Input::Initialization:: *)
makeWrfFcst[basin_,t_, rgn_]:= Module[{dir,difs,rs,n,mod,mlis,models={},mp,dt,mf,ticks,obs,str,ms,ln},
dir = dataDir[basin,t];
SetDirectory[dir];
mlis = FileNames[];
Do[SetDirectory[dir<>"/"<>m];If[Length[FileNames[]]>192,AppendTo[models,m]],{m,mlis}];
(*models= todayModels[basin,t]*);
Print["Todays Models ",models];
If[Length[models]<1, Print["Data not Available"];Abort[]];
ms = Length[models];n=Now;dt = DateObject[{n["Year"],n["Month"],n["Day"],n["Hour"],Quotient[n["Minute"],15]*15,0}];dt =TimeZoneConvert[dt,5.5];
{ticks,obs}=makeVoronoiInRegion[dir,rgn,models,dt];
rs = Get[Last[FileNames[dir<>"rs*"]]];
{mp,mf}= nmd[rs];
Print[mf," model ",mp];
ln=Min[Length /@ rs];
difs =(Last[rs][[ln]]/ #[[ln]])& /@ Most[rs];
Export[dir<>"adjComp.gif",ListPlot[Append[Table[rs[[i]] difs[[i]],{i,1,ms}],rs[[ms+1]]], Ticks -> {ticks,Automatic}, PlotLegends->{models,"Obs"} ,ImageSize->Large, PlotLabel -> "Forecast Multiplied"]];
mod=models[[mp]];
Export[dir<>"twoComp.gif",ListPlot[{rs[[mp]] mf,rs[[ms+1]]}, Ticks -> {ticks,Automatic}, PlotLegends->{mod,"Obs"} ,ImageSize->Large]];
readDirModelAdj[dir,rgn,mod,mf,dt];
str = "{\"model_list\":[[\"WRF_"<>mod<>"\",\"4.1.2,\"gfs_d0_"<>t<>"\","<>ToString[mf]<>"]]}";
Export[dir<>"config.json.txt",str]
]


(* ::Input::Initialization:: *)
writeToCloud[dir_]:= Module[{},
Run["scp -i ~/.ssh/uwcc-admin "<>dir<>"config.json.txt uwcc-admin@35.227.163.211:/home/uwcc-admin/curw_sim_db_utils/rain/config.json"];
Run["scp -i ~/.ssh/uwcc-admin "<>dir<>"WRFgrid.gif uwcc-admin@104.198.0.87:/var/www/html/wrf/"]]


(* ::Input:: *)
(**)


(* ::Input::Initialization:: *)
srgn[rgn_,divx_,divy_]:= Block[{x1,x2,y1,y2,dx,dy,srgns={}},
{{x1,x2},{y1,y2}} = rgn;
dx =( x2-x1)/divx;
dy= (y2-y1) / divy;
Do[
AppendTo[srgns, {{x1+(i-1)dx, x1 + i dx},{y1+(j-1) dy,y1 + dy j}}],{i,1,divx},{j,1,divy}];
srgns]


(* ::Input:: *)
(*Clear[checkStnInRgn]*)


(* ::Input::Initialization:: *)
checkStnInRgn[{{minlon_,maxlon_},{minlat_,maxlat_}},st_,dt_]:=Module[{con,nt,ivl,str,d},
con=OpenSQLConnection[JDBC["MySQL(Connector/J)", "35.227.163.211"],"Name" ->"curw_obs", "Username" -> "admin", "Password" -> "floody","UseConnectionPool"->True];
Print[prDt[st], prDt[dt]];
ivl = Quantity[15,"Minutes"];
str =StringJoin["call curw_obs.getCumulativeObservedRainfallGivenRegion('",prDt[st],"','",prDt[dt],"',",ToString[minlon],",",ToString[maxlon],",",ToString[minlat],",",ToString[maxlat],")"];
d=SQLExecute [con, str];
CloseSQLConnection[con];
d]


(* ::Input::Initialization:: *)
fourGrids = {{{79.84351,79.96084},{6.942291,7.043091}},{{79.84351,79.96084},{6.875092,6.942291}},{{79.84351,79.96084},{6.774292,6.875092}},{{79.96084,80.15639},{6.774292,7.043091}}}


(* ::Input::Initialization:: *)
kelaniRgn={{79.84343675, 80.15575615},{6.774218729,7.042994907}}


(* ::Input:: *)
(*naulaArea = {{80.58, 80.72},{7.65,7.85}}*)


(* ::Input::Initialization:: *)
d03r = {{79.521461, 82.189919}, {5.722969,10.064255}}


twoFcst["18"]
