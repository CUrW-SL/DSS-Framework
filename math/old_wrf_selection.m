models = {"A", "C", "E", "SE"};
models = todayModels[basin, t];
ms = Length[models];
n = Now; dt =
 DateObject[{n["Year"], n["Month"], n["Day"], n["Hour"],
   Quotient[n["Minute"], 15]*15, 0}]; dt = TimeZoneConvert[dt, 5.5];
Rs 🡺 list of cumulative rainfall lists from model 1, to number of models, + observations
n = Length[rs[[ms + 1]]];

--- normalize each cumulative model forecast with respect to observed

difs = Fold[Plus, #] & /@ (Abs@normaliDif[rs[[ms + 1]], #] & /@
     Most[rs]);

Print["Deviation ", difs];
Print["Totals ", rs[[All, n]]];
If[rs[[ms + 1, n]] > 0.,
  difs = Abs[( #[[n]] - rs[[ms + 1, n]])]/(rs[[ms + 1, n]] + .1) & /@
    Most[rs],
  difs = #[[n]] & /@ Most[rs]];
Print["Differences ", difs];
mp = Position[difs, Min[difs]];
difs = Max[rs[[ms + 1, n]], .01]/#[[n]] & /@ Most[rs];
mod = First[Extract[models, mp]];
mf = First[ Extract[difs, mp]];
If[mf <= 0, mf = 1.];
Print["Multipliacation Factors ", difs];
Print["Model ", mod, " mf ", mf];

Function
normaliDif[ob_, fc_] := Module[{n},
  n = Length[ob];
  If[ob[[n]] > 0.,
   If[fc[[n]] > 0.,
    ob/ob[[n]] - Take[fc, {1, n}]/fc[[n]], ob/ob[[n]]],
   Take[fc/fc[[n]], {1, n}]]]
